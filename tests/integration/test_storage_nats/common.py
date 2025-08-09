import asyncio
import logging
import os.path as p
import subprocess
import time
import nats

from helpers.cluster import check_nats_is_available, nats_connect_ssl
from helpers.test_tools import TSV

def wait_nats_to_start(nats_port, ssl_ctx=None, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if asyncio.run(check_nats_is_available(nats_port, ssl_ctx=ssl_ctx)):
                logging.debug("NATS is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to NATS " + str(ex))
            time.sleep(0.5)
    
    assert False, "NATS is unavailable"

# function to check if nats is paused, because in some cases we successfully connected to it after calling pause_container
def wait_nats_paused(nats_port, ssl_ctx=None, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            asyncio.run(check_nats_is_available(nats_port, ssl_ctx=ssl_ctx))
            time.sleep(0.5)
        except nats.errors.NoServersError:
            logging.debug("NATS is paused")
            return
        except Exception as ex:
            logging.warning("Detect NATS status failed with error \"" + str(ex) + "\" - continue waiting for proper status...")
            time.sleep(0.5)
    
    assert False, "NATS is not paused"

def check_query_result(instance, query, time_limit_sec = 60):
    query_result = ""
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        query_result = instance.query(query, ignore_error=True        )
        if check_result(query_result):
            break

    check_result(query_result, True)

def check_result(query_result, check=False, ref_file="test_nats_json.reference"):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(query_result) == TSV(reference)
        else:
            return TSV(query_result) == TSV(reference)


def kill_nats(nats_id):
    p = subprocess.Popen(("docker", "stop", nats_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0

def revive_nats(nats_id, nats_port):
    p = subprocess.Popen(("docker", "start", nats_id), stdout=subprocess.PIPE)
    p.communicate()
    wait_nats_to_start(nats_port)

def create_consumer(cluster_inst, subject, messages=(), bytes=None):
    nc = nats_connect_ssl(
        cluster_inst.nats_port,
        user="click",
        password="house",
        ssl_ctx=cluster_inst.nats_ssl_context,
    )
    logging.debug("NATS connection status: " + str(nc.is_connected))

    for message in messages:
        nc.publish(subject, message.encode())
    if bytes is not None:
        nc.publish(subject, bytes)
    nc.flush()
    logging.debug("Finished publishing to " + subject)

    nc.close()
    return messages

def wait_query_result(instance, query, wait_query_result, sleep_timeout = 0.5, time_limit_sec = 60):
    deadline = time.monotonic() + time_limit_sec
    
    query_result = 0
    while time.monotonic() < deadline:
        query_result = int(instance.query(query))
        if query_result == wait_query_result:
            break
        
        time.sleep(1)
    
    assert query_result == wait_query_result

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
