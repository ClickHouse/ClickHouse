import os.path
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="test_migrate")
node = cluster.add_instance(
    'node',
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


def toggle_fdb(enable):
    if enable:
        with open(os.path.dirname(__file__) + "/configs/foundationdb.xml", "r") as f:
            node.replace_config(
                "/etc/clickhouse-server/config.d/foundationdb.xml", f.read())
    else:
        node.replace_config(
            "/etc/clickhouse-server/config.d/foundationdb.xml", "<clickhouse></clickhouse>")


def test_migrate_from_local():
    # Boot without FDB
    create_function_query1 = "CREATE FUNCTION MyFunc1 AS (a, b) -> a + b"
    create_function_query2 = "CREATE FUNCTION MyFunc2 AS (a, b) -> a * b"

    node.query(create_function_query1)
    node.query(create_function_query2)

    assert node.query("SELECT MyFunc1(1,2)") == "3\n"
    assert node.query("SELECT MyFunc2(1,2)") == "2\n"

    node.stop_clickhouse()
    # First boot with FDB
    toggle_fdb(True)
    node.start_clickhouse()

    assert node.query("SELECT MyFunc1(1,2)") == "3\n"
    assert node.query("SELECT MyFunc2(1,2)") == "2\n"

    # Delete local UDF files
    node.stop_clickhouse()
    toggle_fdb(False)
    node.start_clickhouse()

    node.query("DROP FUNCTION MyFunc1")
    node.query("DROP FUNCTION MyFunc2")

    assert "Unknown function MyFunc1" in node.query_and_get_error(
        "SELECT MyFunc1(1, 2)")
    assert "Unknown function MyFunc2" in node.query_and_get_error(
        "SELECT MyFunc2(1, 2)")

    # Second boot with FDB
    node.stop_clickhouse()
    toggle_fdb(True)
    node.start_clickhouse()

    assert node.query("SELECT MyFunc1(1,2)") == "3\n"
    assert node.query("SELECT MyFunc2(1,2)") == "2\n"


