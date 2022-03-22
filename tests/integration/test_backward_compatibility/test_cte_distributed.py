import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="cte_distributed")
node1 = cluster.add_instance("node1", with_zookeeper=False)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="yandex/clickhouse-server",
    tag="21.7.3.14",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_cte_distributed(start_cluster):
    node2.query(
        """
WITH
    quantile(0.05)(cnt) as p05,
    quantile(0.95)(cnt) as p95,
    p95 - p05 as inter_percentile_range
SELECT 
    sum(cnt) as total_requests,
    count() as data_points,
    inter_percentile_range
FROM (
    SELECT
        count() as cnt
    FROM remote('node{1,2}', numbers(10))
    GROUP BY number
)"""
    )

    node1.query(
        """
WITH
    quantile(0.05)(cnt) as p05,
    quantile(0.95)(cnt) as p95,
    p95 - p05 as inter_percentile_range
SELECT 
    sum(cnt) as total_requests,
    count() as data_points,
    inter_percentile_range
FROM (
    SELECT
        count() as cnt
    FROM remote('node{1,2}', numbers(10))
    GROUP BY number
)"""
    )
