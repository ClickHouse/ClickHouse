import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_persistence():
    create_function_query1 = "CREATE FUNCTION MySum1 AS (a, b) -> a + b"
    create_function_query2 = "CREATE FUNCTION MySum2 AS (a, b) -> MySum1(a, b) + b"

    instance.query(create_function_query1)
    instance.query(create_function_query2)
    
    assert instance.query("SELECT MySum1(1,2)") == "3"
    assert instance.query("SELECT MySum2(1,2)") == "5"

    cluster.shutdown()
    cluster.start()
    instance = cluster.add_instance('instance')

    assert instance.query("SELECT MySum1(1,2)") == "3"
    assert instance.query("SELECT MySum2(1,2)") == "5"

    instance.query("DROP FUNCTION MySum2")
    instance.query("DROP FUNCTION MySum1")

    cluster.shutdown()
    cluster.start()
    instance = cluster.add_instance('instance')

    assert "Unknown function MySum1" in instance.query("SELECT MySum1(1, 2)")
    assert "Unknown function MySum2" in instance.query("SELECT MySum2(1, 2)")
