import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", stay_alive=True)
node2 = cluster.add_instance("node2", stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_cpu_intensive_task(node):
    node.query(
        "SELECT sum(*) FROM system.numbers_mt FORMAT Null SETTINGS max_execution_time=10",
        ignore_error=True,
    )


def get_async_metric(node, metric):
    node.query("SYSTEM FLUSH LOGS")
    return node.query(
        f"""
        SELECT max(value)
            FROM (
            SELECT toStartOfInterval(event_time, toIntervalSecond(1)) AS t, avg(value) AS value
                FROM system.asynchronous_metric_log
            WHERE event_time >= now() - 60 AND metric = '{metric}'
            GROUP BY t
            )
        SETTINGS max_threads = 1
        """
    ).strip("\n")


def test_user_cpu_accounting(start_cluster):
    if node1.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    # run query on the other node, its usage shouldn't be accounted by node1
    run_cpu_intensive_task(node2)

    node1_cpu_time = get_async_metric(node1, "OSUserTime")
    assert float(node1_cpu_time) < 2

    # then let's test that we will account cpu time spent by the server itself
    node2_cpu_time = get_async_metric(node2, "OSUserTime")
    # this check is really weak, but CI is tough place and we cannot guarantee that test process will get many cpu time
    assert float(node2_cpu_time) > 2


def test_normalized_user_cpu(start_cluster):
    if node1.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    # run query on the other node, its usage shouldn't be accounted by node1
    run_cpu_intensive_task(node2)

    node1_cpu_time = get_async_metric(node1, "OSUserTimeNormalized")
    assert float(node1_cpu_time) < 1.01

    node2_cpu_time = get_async_metric(node2, "OSUserTimeNormalized")
    assert float(node2_cpu_time) < 1.01
