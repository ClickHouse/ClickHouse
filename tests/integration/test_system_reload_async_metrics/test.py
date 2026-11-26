import os
import pytest

from helpers.cluster import ClickHouseCluster

# Tests that SYSTEM RELOAD ASYNCHRONOUS METRICS works.

# Config default.xml sets a large refresh interval of asynchronous metrics, so that the periodic updates don't interfere with the manual
# update below.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/default.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")


def test_system_reload_async_metrics(start_cluster):
    node.query("SYSTEM CLEAR QUERY CACHE")
    node.query("DROP TABLE IF EXISTS tab SYNC;")

    # Reload asynchronous metrics to get the latest state (async metrics update period has been set to a large value
    # intentionally in the test config to test the manual reload functionality)
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    # Get table count before creating new table
    res1 = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfTables'"
    )

    # Create table and test that SYSTEM RELOAD ASYNCHRONOUS METRICS reflects the change
    node.query("CREATE TABLE tab (col UInt64) ENGINE MergeTree ORDER BY tuple()")

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    res2 = node.query(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfTables'"
    )

    assert int(res2.strip()) == int(res1.strip()) + 1


FAKE_PROC_STAT = "/tmp/fake_proc_stat"


def write_proc_stat(btime, user, iowait, ctxt, processes, procs_blocked):
    # The server keeps reading this file through the descriptor it opened at startup.
    cpu = f"{user} 0 10000 100000 {iowait} 0 0 0 0 0"
    content = (
        f"cpu  {cpu}\ncpu0 {cpu}\nintr 100000\nctxt {ctxt}\n{btime}\n"
        f"processes {processes}\nprocs_running 1\nprocs_blocked {procs_blocked}\n"
    )
    node.exec_in_container(
        ["bash", "-c", f"printf '%s' '{content}' > {FAKE_PROC_STAT}"], user="root"
    )


def async_metric(name):
    return float(
        node.query(
            f"SELECT value FROM system.asynchronous_metrics WHERE metric = '{name}'"
        )
    )


def test_counter_going_backwards(start_cluster):
    btime = node.exec_in_container(["bash", "-c", "grep '^btime ' /proc/stat"]).strip()
    write_proc_stat(
        btime, user=10000, iowait=5000, ctxt=5000, processes=1000, procs_blocked=7001
    )

    node.stop_clickhouse()
    node.exec_in_container(
        ["mount", "--bind", FAKE_PROC_STAT, "/proc/stat"], user="root"
    )
    try:
        node.start_clickhouse()
    finally:
        node.exec_in_container(["umount", "--lazy", "/proc/stat"], user="root")
    assert async_metric("OSProcessesBlocked") == 7001

    # iowait (proc(5): "may decrease") and ctxt step backwards, user and processes advance.
    write_proc_stat(
        btime, user=10500, iowait=4000, ctxt=4000, processes=1007, procs_blocked=7002
    )
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    assert async_metric("OSProcessesBlocked") == 7002
    assert async_metric("OSIOWaitTime") == 0
    assert async_metric("OSContextSwitches") == 0
    assert async_metric("OSProcessesCreated") == 7
    assert async_metric("OSUserTime") > 0
