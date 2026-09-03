import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import get_fake_zk

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=False,
    use_keeper=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_no_slow_log_from_poll_idle_time(started_cluster):
    """
    Verify that time spent waiting in poll() is not attributed to subsequent
    operations like "Receiving request".

    Before the fix, the logging_stopwatch was started before poll() and not
    restarted after it returned. When poll() blocked for a long time (e.g.
    between heartbeats), the elapsed time was incorrectly attributed to
    "Receiving request", producing noisy and misleading INFO messages.
    """
    zk = get_fake_zk(cluster, "node", timeout=5)

    try:
        # Perform some operations to establish the session
        zk.ensure_path("/test_slow_log")
        zk.set("/test_slow_log", b"data")
        zk.get("/test_slow_log")

        # Wait for several heartbeat/poll cycles.
        # With session_timeout=5000ms, the client sends heartbeats roughly
        # every ~1.6s. Between heartbeats poll() blocks. Without the fix,
        # each heartbeat receipt after a long poll() would log
        # "Receiving request for session X took ~1600ms" (exceeding the
        # 500ms threshold we configured).
        time.sleep(5)

        # More operations after the idle period
        zk.set("/test_slow_log", b"updated")
        zk.get("/test_slow_log")

        # Give a bit of time for log to flush
        time.sleep(1)

        # With the fix, poll() idle time is not attributed to "Receiving
        # request", so this message should not appear for normal operations.
        assert not node.contains_in_log(
            "Receiving request for session"
        ), "poll() idle time leaked into 'Receiving request' log message"
    finally:
        zk.stop()
        zk.close()
