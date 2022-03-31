#!/usr/bin/env python3

# Copyright (c) 2019 Princeton University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Standard imports
import json
from optparse import OptionParser
import os
from os.path import exists as file_exists
import requests
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
import subprocess
import sys
import time
import threading
import logging

# Local imports
sys.path = ['./', '../'] + sys.path
from GenConfigs import *
sys.path = [FAAS_ROOT + '/synthetic_workload_invoker'] + sys.path
from EventGenerator import GenericEventGenerator
from commons.JSONConfigHelper import CheckJSONConfig, ReadJSONConfig
from commons.Logger import ScriptLogger
from WorkloadChecker import CheckWorkloadValidity

logging.captureWarnings(True)
logger = ScriptLogger('workload_invoker', 'SWI.log')


APIHOST = subprocess.check_output(WSK_PATH + " property get --apihost", shell=True).split()[3].decode('utf-8')
# APIHOST = 'https://' + APIHOST.decode("utf-8")
AUTH_KEY = subprocess.check_output(WSK_PATH + " property get --auth", shell=True).split()[2].decode("utf-8")
# AUTH_KEY = AUTH_KEY.decode("utf-8")
user_pass = AUTH_KEY.split(':')
NAMESPACE = subprocess.check_output(WSK_PATH + " property get --namespace", shell=True).split()[2].decode("utf-8")
# NAMESPACE = NAMESPACE.decode("utf-8")
RESULT = 'false'
base_url = APIHOST + '/api/v1/namespaces/' + NAMESPACE + '/actions/'
base_gust_url = APIHOST + '/api/v1/web/guest/default/'

param_file_cache = {}   # a cache to keep json of param files
binary_data_cache = {}  # a cache to keep binary data (image files, etc.)


def PROCESSInstanceGenerator(instance, instance_script, instance_times, blocking_cli):
    if len(instance_times) == 0:
        return False
    after_time, before_time = 0, 0

    if blocking_cli:
        pass
    else:
        instance_script = instance_script + ' &'

    for t in instance_times:
        time.sleep(max(0, t - (after_time - before_time)))
        before_time = time.time()
        os.system(instance_script)
        after_time = time.time()

    return True


def HTTPInstanceGenerator(action, instance_times, blocking_cli, param_file=None):
    if len(instance_times) == 0:
        return False
    session = FuturesSession(max_workers=15)
    url = base_url + action
    parameters = {'blocking': blocking_cli, 'result': RESULT}
    authentication = (user_pass[0], user_pass[1])
    after_time, before_time = 0, 0

    if param_file == None:
        st = 0
        for t in instance_times:
            st = st + t - (after_time - before_time)
            before_time = time.time()
            if st > 0:
                time.sleep(st)
            future = session.post(url, params=parameters, auth=authentication, verify=False)
            after_time = time.time()
    else:   # if a parameter file is provided
        try:
            param_file_body = param_file_cache[param_file]
        except:
            with open(param_file, 'r') as f:
                param_file_body = json.load(f)
                param_file_cache[param_file] = param_file_body

        for t in instance_times:
            st = t - (after_time - before_time)
            if st > 0:
                time.sleep(st)
            before_time = time.time()
            future = session.post(url, params=parameters, auth=authentication,
                                  json=param_file_body, verify=False)
            after_time = time.time()

    return True


def BinaryDataHTTPInstanceGenerator(action, instance_times, blocking_cli, data_file):
    """
    TODO: Automate content type
    """
    url = base_gust_url + action
    session = FuturesSession(max_workers=15)
    if len(instance_times) == 0:
        return False
    after_time, before_time = 0, 0

    try:
        data = binary_data_cache[data_file]
    except:
        data = open(data_file, 'rb').read()
        binary_data_cache[data_file] = data

    for t in instance_times:
        st = t - (after_time - before_time)
        if st > 0:
            time.sleep(st)
        before_time = time.time()
        session.post(url=url, headers={'Content-Type': 'image/jpeg'},
                     params={'blocking': blocking_cli, 'result': RESULT},
                     data=data, auth=(user_pass[0], user_pass[1]), verify=False)
        after_time = time.time()

    return True

def KnativeInstanceGenerator(action:str, instance_times:list, blocking_cli:bool, param_file=None) -> bool:
    # there has to be a list of instance times for calling this function
    if len(instance_times) == 0:
        logger.error("No instance times provided; got: None for instance_times argument")
        return False
    
    # read params for this action if not in cache
    params = param_file_cache.get(param_file, None)
    if param_file != None and params == None:
        if not file_exists(param_file):
            logger.error("param_file does not exist; got: {} for param_file argument".format(param_file))
            return False

        with open(file=param_file, mode='r') as f:
            try:
                params = json.load(f)
                param_file_cache[param_file.strip()] = params
            except Exception as e:
                logger.error("Failed to read param_file {}: {}".format(param_file, e))
                return False

    # getting service name and url for the 'action' 
    cmd_kn_list = 'kn service ls {0}'
    action_name_url_str = subprocess.check_output([cmd_kn_list.format(action)], shell=True).decode("utf-8").splitlines()
    if len(action_name_url_str) < 2:
        logger.error("No service found for action: {0}".format(action))
        logger.error("'kn service ls {0}' returned {1}".format(action, action_name_url_str))
        logger.error("make sure have created the service for action '{0}' using 'kn service create'".format(action))
        return False
    # first line is header; ignore it.
    action_name_url_str = action_name_url_str[1]
    
    # The second line is the service.
    # The first two columns are the service name and the service url, respectively.
    srv_name, srv_url  = action_name_url_str.split()[:2]

    logger.info("calling the action using kn")
    logger.info("service={0} @ {1}), params={2}, instance_times={3} blocking={4}".format(srv_name, srv_url, params, instance_times, blocking_cli))

    session = FuturesSession(max_workers=28)
    called_at_ts, sleep_time, after_time, before_time = 0, 0, 0, 0
    futures = [0]*len(instance_times)

    for idx, t in enumerate(instance_times):
        called_at_ts += t
        sleep_time = sleep_time + t - (after_time - before_time)
        
        before_time = time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)
        
        # todo: what if it is a post?
        f = session.get(url=srv_url, params=params)
        after_time = time.time()

        futures[idx] = f

        # save call time and the params
        f.call_num = idx
        f.param_file = param_file
        f.sleep_time = sleep_time
        f.called_at_ts = called_at_ts
    
    if blocking_cli:
        for f in as_completed(futures):
            logger.info("[call num: {0}] @ call time={1} ".format(f.call_num, f.called_at_ts))



def main(argv):
    """
    The main function.
    """
    logger.info("Workload Invoker started")
    print("Log file -> logs/SWI.log")
    parser = OptionParser()
    parser.add_option("-c", "--config_json", dest="config_json",
                      help="The input json config file describing the synthetic workload.", metavar="FILE")
    (options, args) = parser.parse_args()

    if not CheckJSONConfig(options.config_json):
        logger.error("You should specify a JSON config file using -c option!")
        return False    # Abort the function if json file not valid

    workload = ReadJSONConfig(options.config_json)
    if not CheckWorkloadValidity(workload=workload):
        return False    # Abort the function if json file not valid

    monitoring_script_path = FAAS_ROOT
    runtime_script = workload['perf_monitoring'].get('runtime_script', None)
    if runtime_script != None:
        monitoring_script_path = os.path.join(monitoring_script_path, runtime_script)
        
        if not file_exists(monitoring_script_path):
            logger.error("Monitoring script does not exist at {}".format(monitoring_script_path))
            return False

    if workload['platform'] == 'knative':
        logger.info("Knative platform detected")
        blocking_cli = workload['blocking_cli'] if workload['blocking_cli'] != None else False
        logger.info(msg="setting the blocking_cli to {}".format(blocking_cli))


    [all_events, event_count] = GenericEventGenerator(workload)
    threads = []

    if workload['platform'] == 'knative':
        logger.info("Knative platform detected")
        for (instance_name, instance_times) in all_events.items():
            # reading the instance object
            instance = workload['instances'][instance_name]
            
            param_file = None if not 'param_file' in instance else instance['param_file']
            data_file = None if not 'data_file' in instance else instance['data_file']
            
            if data_file is None:
                threads.append(threading.Thread(target=KnativeInstanceGenerator, args=[
                           instance['application'], instance_times, blocking_cli, param_file]))        
            else:
                threads.append(threading.Thread(target=KnativeInstanceGenerator, args=[
                           instance['application'], instance_times, blocking_cli, data_file]))   

    # all_events: dict(event_name: list(instance_times))
    for (instance, instance_times) in all_events.items():
        action = workload['instances'][instance]['application']
        try:
            param_file = workload['instances'][instance]['param_file']
        except:
            param_file = None
        blocking_cli = workload['blocking_cli']
        if 'data_file' in workload['instances'][instance].keys():
            data_file = workload['instances'][instance]['data_file']
            threads.append(threading.Thread(target=BinaryDataHTTPInstanceGenerator, args=[
                           action, instance_times, blocking_cli, data_file]))
        else:
            threads.append(threading.Thread(target=HTTPInstanceGenerator, args=[
                           action, instance_times, blocking_cli, param_file]))
        pass

    # Dump Test Metadata
    os.system("date +%s%N | cut -b1-13 > " + FAAS_ROOT +
              "/synthetic_workload_invoker/test_metadata.out")
    os.system("echo " + options.config_json + " >> " + FAAS_ROOT +
              "/synthetic_workload_invoker/test_metadata.out")
    os.system("echo " + str(event_count) + " >> " + FAAS_ROOT +
              "/synthetic_workload_invoker/test_metadata.out")

    try:
        if runtime_script != None:
            logger.info("Running monitoring script...")
            exit_code = os.system("bash {} {} &".format(runtime_script, workload['test_duration_in_seconds']))
            logger.info("Monitoring script ran with exit code: {}".format(exit_code))
    except Exception as e:
        logger.error("Failed to start monitoring script: {}".format(e))

    logger.info("Test started")
    for thread in threads:
        thread.start()
    logger.info("Test ended")

    return True


if __name__ == "__main__":
    main(sys.argv)
