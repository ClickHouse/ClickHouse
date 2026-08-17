import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/thread_pool.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/thread_pool.xml"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_sufficient_thread_pool_size_starts(start_cluster):
    assert node.get_process_pid("clickhouse") is not None
    assert node.query("SELECT 1") == "1\n"


def test_too_small_thread_pool_size_fails_startup(start_cluster):
    # The server permanently occupies a few hundred threads of the global thread pool for the workers of
    # the background pools, the background schedule pools and the flush threads of the system logs. A
    # `max_thread_pool_size` that cannot hold them has to be reported, and the server has to terminate.
    #
    # It used to hang instead: a `ThreadFromGlobalPool` that could not get a thread was silently put
    # into the queue of the global pool, where nothing would ever pick it up, because the threads are
    # held by other such jobs that never return. The startup then waited forever for the loader worker
    # of the `system` database, which was one of the jobs stuck in that queue. `expected_to_fail` here
    # asserts that the process really exits, so a hang fails this test rather than timing out silently.
    node.stop_clickhouse()
    # `160` reaches the late-start saturation band: early startup succeeds, but
    # a permanent worker started afterwards must still fail fast rather than be
    # silently queued forever.
    node.replace_in_config(CONFIG_PATH, "10000", "160")
    node.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    assert node.get_process_pid("clickhouse") is None
    assert node.contains_in_log("Cannot schedule a task")
    assert node.contains_in_log("max_thread_pool_size")

    # Restore a working value so the server (and module teardown) is healthy again.
    node.replace_in_config(CONFIG_PATH, "160", "10000")
    node.start_clickhouse()
    assert node.get_process_pid("clickhouse") is not None
    assert node.query("SELECT 1") == "1\n"
