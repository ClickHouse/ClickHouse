import os
import time

import pytest

import helpers.cluster
import helpers.test_tools

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_node():
    cluster = helpers.cluster.ClickHouseCluster(__file__)
    try:
        node = cluster.add_instance(
            "node", main_configs=["configs/crash_log.xml"], stay_alive=True
        )

        cluster.start()
        yield node
    finally:
        cluster.shutdown()


def send_signal(started_node, signal):
    started_node.exec_in_container(
        ["bash", "-c", f"pkill -{signal} clickhouse"], user="root"
    )


def wait_for_clickhouse_stop(started_node):
    result = None
    for attempt in range(120):
        time.sleep(1)
        pid = started_node.get_process_pid("clickhouse")
        if pid is None:
            result = "OK"
            break
    assert result == "OK", "ClickHouse process is still running"


def test_pkill(started_node):
    if (
        started_node.is_built_with_thread_sanitizer()
        or started_node.is_built_with_address_sanitizer()
        or started_node.is_built_with_memory_sanitizer()
    ):
        pytest.skip("doesn't fit in timeouts for stacktrace generation")

    crashes_count = 0
    for signal in ["SEGV", "4"]:
        send_signal(started_node, signal)
        wait_for_clickhouse_stop(started_node)
        started_node.restart_clickhouse()
        crashes_count += 1
        assert (
            started_node.query("SELECT COUNT(*) FROM system.crash_log")
            == f"{crashes_count}\n"
        )


def test_pkill_query_log(started_node):
    if (
        started_node.is_built_with_thread_sanitizer()
        or started_node.is_built_with_address_sanitizer()
        or started_node.is_built_with_memory_sanitizer()
    ):
        pytest.skip("doesn't fit in timeouts for stacktrace generation")

    for signal in ["SEGV", "4"]:
        # force create query_log if it was not created
        started_node.query("SYSTEM FLUSH LOGS")
        started_node.query("TRUNCATE TABLE IF EXISTS system.query_log")
        started_node.query("SELECT COUNT(*) FROM system.query_log")
        # logs don't flush
        assert started_node.query("SELECT COUNT(*) FROM system.query_log") == f"{0}\n"

        send_signal(started_node, signal)
        wait_for_clickhouse_stop(started_node)
        started_node.restart_clickhouse()
        assert started_node.query("SELECT COUNT(*) FROM system.query_log") >= f"3\n"
