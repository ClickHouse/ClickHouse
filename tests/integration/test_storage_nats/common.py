import asyncio
import logging
import os.path as p
import subprocess
import time
import nats

from helpers.cluster import check_nats_is_available, nats_connect_ssl
from helpers.test_tools import TSV
from helpers.config_cluster import nats_user, nats_pass

def wait_nats_to_start(cluster, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if asyncio.run(check_nats_is_available(cluster)):
                logging.debug("NATS is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to NATS " + str(ex))
            time.sleep(0.5)
    
    assert False, "NATS is unavailable"

# function to check if nats is paused, because in some cases we successfully connected to it after calling pause_container
def wait_nats_paused(cluster, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            asyncio.run(check_nats_is_available(cluster))
            time.sleep(0.5)
        except nats.errors.NoServersError:
            logging.debug("NATS is paused")
            return
        except Exception as ex:
            logging.warning("Detect NATS status failed with error \"" + str(ex) + "\" - continue waiting for proper status...")
            time.sleep(0.5)
    
    assert False, "NATS is not paused"

def check_query_result(instance, query, retry_count=60):
    result = instance.query_with_retry(query, retry_count=retry_count, ignore_error=True, check_callback=lambda result: check_result(result))
    check_result(result, True)

def check_result(query_result, check=False, ref_file="test_nats_json.reference"):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(query_result) == TSV(reference)
        else:
            return TSV(query_result) == TSV(reference)


def kill_nats(cluster):
    p = subprocess.Popen(("docker", "stop", cluster.nats_docker_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0

def revive_nats(cluster):
    p = subprocess.Popen(("docker", "start", cluster.nats_docker_id), stdout=subprocess.PIPE)
    p.communicate()
    wait_nats_to_start(cluster)

def wait_query_result(instance, query, wait_query_result, sleep_timeout = 0.5, retry_count = 60):
    query_result = instance.query_with_retry(
        query, 
        retry_count=retry_count, 
        sleep_time=sleep_timeout, 
        ignore_error=True, 
        check_callback=lambda result: int(result) == wait_query_result)
    
    assert int(query_result) == wait_query_result

def wait_for_table_is_ready(instance, table_name, sleep_timeout = 0.5, time_limit_sec = 60):
    deadline = time.monotonic() + time_limit_sec
    while (not check_table_is_ready(instance, table_name)) and time.monotonic() < deadline:
        time.sleep(sleep_timeout)

    assert(check_table_is_ready(instance, table_name))

# waiting for subscription to nats subjects (after subscription direct selection is not available and completed with an error)
def wait_for_mv_attached_to_table(instance, table_name, sleep_timeout = 0.5, time_limit_sec = 60):
    deadline = time.monotonic() + time_limit_sec
    while check_table_is_ready(instance, table_name) and time.monotonic() < deadline:
        time.sleep(sleep_timeout)
    
    assert(not check_table_is_ready(instance, table_name))

def check_table_is_ready(instance, table_name):
    try:
        instance.query("SELECT * FROM {}".format(table_name))
        return True
    except Exception:
        return False
