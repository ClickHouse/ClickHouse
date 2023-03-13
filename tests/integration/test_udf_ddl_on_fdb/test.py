import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="basic_test")
node = cluster.add_instance(
    'node',
    main_configs=["configs/foundationdb.xml"],
    with_foundationdb=True,
    stay_alive=True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_create():
    create_function_query1 = "CREATE FUNCTION MySum1 AS (a, b) -> a + b"
    create_function_query2 = "CREATE FUNCTION MySum2 AS (a, b) -> MySum1(a, b) + b"

    node.query(create_function_query1)
    node.query(create_function_query2)

    assert node.query("SELECT MySum1(1,2)") == "3\n"
    assert node.query("SELECT MySum2(1,2)") == "5\n"


def test_drop():
    drop_function_query1 = "DROP FUNCTION MySum1"
    drop_function_query2 = "DROP FUNCTION MySum2"

    node.query(drop_function_query1)
    node.query(drop_function_query2)

    assert "Unknown function MySum1" in node.query_and_get_error(
        "SELECT MySum1(1, 2)")
    assert "Unknown function MySum2" in node.query_and_get_error(
        "SELECT MySum2(1, 2)")
