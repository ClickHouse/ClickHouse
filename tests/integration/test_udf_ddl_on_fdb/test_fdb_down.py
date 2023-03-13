import pytest
from helpers.cluster import ClickHouseCluster
import time

cluster = ClickHouseCluster(__file__, name="fdb_down")
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
        time.sleep(30) # wait first boot done
        yield cluster

    finally:
        cluster.shutdown()

def assert_timeout():
    return pytest.raises(Exception, match="Operation aborted because the transaction timed out")


def test_create():
    create_function_query = "CREATE FUNCTION MyFunction1 AS (a, b) -> a + b"
    cluster.stop_fdb()
    with assert_timeout():
        node.query(create_function_query)
    assert "Unknown function MyFunction1" in node.query_and_get_error(
        "SELECT MyFunction1(1, 2)")

    cluster.start_fdb()
    node.query(create_function_query)
    assert node.query("SELECT MyFunction1(1,2)") == "3\n"


def test_drop():

    create_function_query = "CREATE FUNCTION MyFunction2 AS (a, b) -> a * b"
    drop_function_query = "DROP FUNCTION MyFunction2"

    node.query(create_function_query)
    cluster.stop_fdb()
    with assert_timeout():
        node.query(drop_function_query)

    assert node.query("SELECT MyFunction2(1,2)") == "2\n"

    cluster.start_fdb()

    node.query(drop_function_query)
    assert "Unknown function MyFunction2" in node.query_and_get_error(
        "SELECT MyFunction2(1,2)")
