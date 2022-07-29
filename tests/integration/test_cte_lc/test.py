import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_lc_of_string(start_cluster):
    result = node.query("WITH (  SELECT toLowCardinality('a') ) AS bar SELECT bar")

    assert result == "a\n"


def test_lc_of_int(start_cluster):
    result = node.query("WITH (  SELECT toLowCardinality(1) ) AS bar SELECT bar")

    assert result == "1\n"
