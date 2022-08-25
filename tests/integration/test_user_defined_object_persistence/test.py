import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


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

    assert instance.query("SELECT MySum1(1,2)") == "3\n"
    assert instance.query("SELECT MySum2(1,2)") == "5\n"

    instance.restart_clickhouse()

    assert instance.query("SELECT MySum1(1,2)") == "3\n"
    assert instance.query("SELECT MySum2(1,2)") == "5\n"

    instance.query("DROP FUNCTION MySum2")
    instance.query("DROP FUNCTION MySum1")

    instance.restart_clickhouse()

    assert "Unknown function MySum1" in instance.query_and_get_error(
        "SELECT MySum1(1, 2)"
    )
    assert "Unknown function MySum2" in instance.query_and_get_error(
        "SELECT MySum2(1, 2)"
    )
