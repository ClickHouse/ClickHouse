import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1", main_configs=["configs/config.d/clusters.xml"], with_zookeeper=True
)
ch2 = cluster.add_instance(
    "ch2", main_configs=["configs/config.d/clusters.xml"], with_zookeeper=True
)
ch3 = cluster.add_instance(
    "ch3", main_configs=["configs/config.d/clusters.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_sql_user_defined_functions_on_cluster():
    assert "Unknown function test_function" in ch1.query_and_get_error(
        "SELECT test_function(1);"
    )
    assert "Unknown function test_function" in ch2.query_and_get_error(
        "SELECT test_function(1);"
    )
    assert "Unknown function test_function" in ch3.query_and_get_error(
        "SELECT test_function(1);"
    )

    ch1.query_with_retry(
        "CREATE FUNCTION test_function ON CLUSTER 'cluster' AS x -> x + 1;"
    )

    assert ch1.query("SELECT test_function(1);") == "2\n"
    assert ch2.query("SELECT test_function(1);") == "2\n"
    assert ch3.query("SELECT test_function(1);") == "2\n"

    ch2.query_with_retry("DROP FUNCTION test_function ON CLUSTER 'cluster'")
    assert "Unknown function test_function" in ch1.query_and_get_error(
        "SELECT test_function(1);"
    )
    assert "Unknown function test_function" in ch2.query_and_get_error(
        "SELECT test_function(1);"
    )
    assert "Unknown function test_function" in ch3.query_and_get_error(
        "SELECT test_function(1);"
    )
