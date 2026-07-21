import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=False)
node2 = cluster.add_instance("node2", with_zookeeper=False)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_log_quries_probability_one(start_cluster):
    for i in range(100):
        node1.query("SELECT 12345", settings={"log_queries_probability": 0.5})

    node1.query("SYSTEM FLUSH LOGS")

    assert (
        node1.query(
            "SELECT count() < (2 * 100) FROM system.query_log WHERE query LIKE '%12345%' AND query NOT LIKE '%system.query_log%'"
        )
        == "1\n"
    )
    assert (
        node1.query(
            "SELECT count() > 0         FROM system.query_log WHERE query LIKE '%12345%' AND query NOT LIKE '%system.query_log%'"
        )
        == "1\n"
    )
    assert (
        node1.query(
            "SELECT count() % 2         FROM system.query_log WHERE query LIKE '%12345%' AND query NOT LIKE '%system.query_log%'"
        )
        == "0\n"
    )

    node1.query("TRUNCATE TABLE system.query_log")


def test_log_quries_probability_two(start_cluster):
    for i in range(100):
        node1.query(
            "SELECT 12345 FROM remote('node2', system, one)",
            settings={"log_queries_probability": 0.5},
        )

        node1.query("SYSTEM FLUSH LOGS")
        node2.query("SYSTEM FLUSH LOGS")

        ans1 = node1.query(
            "SELECT count() FROM system.query_log WHERE query LIKE '%12345%' AND query NOT LIKE '%system.query_log%'"
        )
        ans2 = node2.query(
            "SELECT count() FROM system.query_log WHERE query LIKE '%12345%' AND query NOT LIKE '%system.query_log%'"
        )

        assert ans1 == ans2

        node1.query("TRUNCATE TABLE system.query_log")
        node2.query("TRUNCATE TABLE system.query_log")
