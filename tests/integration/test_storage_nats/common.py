import asyncio
import logging
import os.path as p
import shlex
import subprocess
import time
import nats

from helpers.cluster import check_nats_is_available
from helpers.cluster import nats_connect_ssl as nats_connect_ssl  # re-exported for the test modules
from helpers.test_tools import TSV

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

def hard_kill_nats(cluster):
    # `SIGKILL`, so the broker answers nothing on its way out: unlike `kill_nats`, which stops it
    # gracefully, the client is left holding subscriptions it has no status for.
    p = subprocess.Popen(("docker", "kill", cluster.nats_docker_id), stdout=subprocess.PIPE)
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

    # Refusing a direct `SELECT` only proves that a materialized view is attached; the
    # background task subscribes to the subjects slightly later. Core NATS has no backlog, so
    # a message published in that window is dropped and never reaches the view.
    wait_for_streaming_started(instance, table_name, time_limit_sec)

STREAMING_STARTED_LOG_LINE = "Started streaming to [0-9]+ attached views"

def wait_for_streaming_started(instance, table_name, time_limit_sec = 60, anchor = None, sleep_timeout = 0.2):
    # The background task logs the line on every iteration, right after the consumer has
    # subscribed, so a fresh occurrence means the subscription is live now. Tests within a
    # module reuse table names, hence the wait has to tell a fresh occurrence from the ones the
    # previous tests left behind.
    #
    # `anchor` - an absolute log offset from `log_line_count`, taken before whatever makes the
    # table stream - draws that line exactly, and is the only reliable way to draw it for a table
    # with a long flush interval. Without it the earlier occurrences are counted in a tail window
    # that keeps sliding as the log grows, so a line written between that count and the wait is not
    # new to the window the wait then looks at: it evicted an older match instead of adding to one.
    # The wait then needs the streaming cycle after it, a whole flush interval away.
    log_line = "{}.*{}".format(table_name, STREAMING_STARTED_LOG_LINE)

    if anchor is None:
        seen = count_in_recent_log(instance, log_line)
        instance.wait_for_log_line(log_line, timeout=time_limit_sec, repetitions=seen + 1)
        return

    deadline = time.monotonic() + time_limit_sec
    while time.monotonic() < deadline:
        if count_in_log_after(instance, log_line, anchor) > 0:
            return
        time.sleep(sleep_timeout)

    raise AssertionError(
        "{} did not start streaming within {} seconds".format(table_name, time_limit_sec))

def count_in_recent_log(instance, pattern, look_behind_lines = 10000):
    result = instance.exec_in_container(
        [
            "bash",
            "-c",
            "tail -n{} /var/log/clickhouse-server/clickhouse-server.log | grep -Ec {} || true".format(
                look_behind_lines, shlex.quote(pattern)
            ),
        ]
    )
    return int(result.strip() or 0)

def log_line_count(instance):
    # Absolute line count of the whole log, to be used as an anchor for `count_in_log_after`.
    result = instance.exec_in_container(
        ["bash", "-c", "wc -l < /var/log/clickhouse-server/clickhouse-server.log"]
    )
    return int(result.strip() or 0)

def count_in_log_after(instance, pattern, after_line):
    # Counts matches strictly after an absolute line offset, so a count of zero really means "no
    # such line was written since the anchor". Unlike `count_in_recent_log`, which looks at a
    # fixed-size tail, this cannot lose an older match as the log grows.
    #
    # An absolute offset only goes stale if the log rotates between the anchor and the count. The
    # integration config rotates at 1000M keeping 10 files
    # (`helpers/0_common_instance_config.xml`), which is hours away at these tests' log rate, and
    # callers assert the log has not shrunk below the anchor so a rotation fails loudly.
    result = instance.exec_in_container(
        [
            "bash",
            "-c",
            "tail -n +{} /var/log/clickhouse-server/clickhouse-server.log | grep -Ec {} || true".format(
                after_line + 1, shlex.quote(pattern)
            ),
        ]
    )
    return int(result.strip() or 0)

def check_table_is_ready(instance, table_name):
    try:
        instance.query("SELECT * FROM {}".format(table_name))
        return True
    except Exception:
        return False
