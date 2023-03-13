import os.path
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="test_migrate_fdb_down")
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


def test_migrate_from_local_when_fdb_down():
    # Boot without fdb
    create_function_query1 = "CREATE FUNCTION MyFunc1 AS (a, b) -> a + b"
    create_function_query2 = "CREATE FUNCTION MyFunc2 AS (a, b) -> a * b"
    node.query(create_function_query1)
    node.stop_clickhouse()

    # First boot with fdb, but fdb is down
    toggle_fdb(True)
    cluster.stop_fdb()
    with pytest.raises(Exception, match="Cannot start ClickHouse"):
        node.start_clickhouse(30)
    assert node.contains_in_log(
        "Operation aborted because the transaction timed out")

    # Disable fdb and change local data
    node.stop_clickhouse()
    toggle_fdb(False)
    node.start_clickhouse()

    assert node.query("SELECT MyFunc1(1,2)") == "3\n"
    node.query(create_function_query2)
    assert node.query("SELECT MyFunc2(1,2)") == "2\n"

    # Second boot with fdb
    node.stop_clickhouse()
    toggle_fdb(True)
    cluster.start_fdb()
    node.start_clickhouse()
    assert node.query("SELECT MyFunc1(1,2)") == "3\n"
    assert node.query("SELECT MyFunc2(1,2)") == "2\n"
