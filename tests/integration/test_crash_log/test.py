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
        cluster.shutdown(ignore_fatal=True)


def send_signal(started_node, signal):
    started_node.exec_in_container(
        ["bash", "-c", f"pkill -{signal} clickhouse"], user="root"
    )


def wait_for_clickhouse_stop(started_node):
    result = None
    for attempt in range(180):
        time.sleep(1)
        pid = started_node.get_process_pid("clickhouse")
        if pid is None:
            result = "OK"
            break
    assert result == "OK", "ClickHouse process is still running"


def test_crash_log_synchronous(started_node):
    started_node.query("TRUNCATE TABLE IF EXISTS system.crash_log")

    crashes_count = 0
    for signal in ["SEGV", "4"]:
        started_node.query("SYSTEM ENABLE FAILPOINT sleep_in_logs_flush")
        send_signal(started_node, signal)
        wait_for_clickhouse_stop(started_node)
        started_node.restart_clickhouse()
        crashes_count += 1
        assert (
            started_node.query("SELECT COUNT(*) FROM system.crash_log")
            == f"{crashes_count}\n"
        )


@pytest.mark.parametrize(
    "failpoint",
    [
        "terminate_with_exception",
        "terminate_with_std_exception",
    ]
)
def test_crash_log_extra_fields(started_node, failpoint):
    started_node.query("TRUNCATE TABLE IF EXISTS system.crash_log")
    started_node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    started_node.query("SELECT 1", ignore_error=True)
    wait_for_clickhouse_stop(started_node)
    started_node.restart_clickhouse()

    assert started_node.query(
        """
        SELECT
            count()
        FROM system.crash_log
        WHERE 1
            AND signal = 6
            AND signal_code = -6 -- SI_TKILL
            AND signal_description = 'Sent by tkill.'
            AND fault_access_type = ''
            AND fault_address IS NULL
            AND arrayExists(x -> x LIKE '%executeQuery%', current_exception_trace_full)
            AND query = 'SELECT 1'
            AND length(git_hash) > 0
            AND length(architecture) > 0
        """
    ).strip() == "1"


def test_pkill_query_log(started_node):
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
