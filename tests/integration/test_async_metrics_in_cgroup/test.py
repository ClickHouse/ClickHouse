import pytest
import subprocess
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_user_cpu_accounting(start_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    # check that our metrics sources actually exist
    assert (
        subprocess.Popen("test -f /sys/fs/cgroup/cpu.stat".split(" ")).wait() == 0
        or subprocess.Popen(
            "test -f /sys/fs/cgroup/cpuacct/cpuacct.stat".split(" ")
        ).wait()
        == 0
    )

    # first let's spawn some cpu-intensive process outside of the container and check that it doesn't accounted by ClickHouse server
    proc = subprocess.Popen(
        "openssl speed -multi 8".split(" "),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    time.sleep(5)

    metric = node.query(
        """
      SELECT max(value)
        FROM (
          SELECT toStartOfInterval(event_time, toIntervalSecond(1)) AS t, avg(value) AS value
            FROM system.asynchronous_metric_log
           WHERE event_time >= now() - 60 AND metric = 'OSUserTime'
        GROUP BY t
        )
    """
    ).strip("\n")

    assert float(metric) < 2

    proc.kill()

    # then let's test that we will account cpu time spent by the server itself
    node.query(
        "SELECT cityHash64(*) FROM system.numbers_mt FORMAT Null SETTINGS max_execution_time=10",
        ignore_error=True,
    )

    metric = node.query(
        """
      SELECT max(value)
        FROM (
          SELECT toStartOfInterval(event_time, toIntervalSecond(1)) AS t, avg(value) AS value
            FROM system.asynchronous_metric_log
           WHERE event_time >= now() - 60 AND metric = 'OSUserTime'
        GROUP BY t
        )
    """
    ).strip("\n")

    # this check is really weak, but CI is tough place and we cannot guarantee that test process will get many cpu time
    assert float(metric) > 1
