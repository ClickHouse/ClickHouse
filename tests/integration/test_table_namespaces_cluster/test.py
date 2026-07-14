import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_explicit_qualifier_ships_as_written(started_cluster):
    """
    A database existing only on one worker must not be reinterpreted as a namespace
    path by the other nodes: DROP ... ON CLUSTER targets the written name everywhere.
    """
    node2.query("CREATE DATABASE remote_db")
    node2.query("CREATE TABLE remote_db.t (x UInt8) ENGINE = Memory")
    # a lexical shadow on node1: same spelling as the namespace interpretation
    node1.query("CREATE TABLE default.`remote_db.t` (x UInt8) ENGINE = Memory")

    node1.query(
        "DROP TABLE remote_db.t ON CLUSTER two_nodes",
        settings={"distributed_ddl_output_mode": "null_status_on_timeout"},
    )

    assert node2.query("EXISTS TABLE remote_db.t").strip() == "0"
    # the shadow table must be untouched
    assert node1.query("EXISTS TABLE default.`remote_db.t`").strip() == "1"

    node1.query("DROP TABLE default.`remote_db.t`")
    node2.query("DROP DATABASE remote_db")


def test_create_as_select_bakes_scope(started_cluster):
    """
    CREATE ... ON CLUSTER AS SELECT under USE db.ns must ship the scoped source, so
    workers without the scope read the same table as the initiator.
    """
    for node in (node1, node2):
        node.query("CREATE TABLE default.`ns.src` (x UInt8) ENGINE = Memory")
    node1.query("INSERT INTO default.`ns.src` VALUES (1), (2)")
    node2.query("INSERT INTO default.`ns.src` VALUES (3)")
    # a root-level decoy: pre-fix workers would read this instead
    node2.query("CREATE TABLE default.src (x UInt8) ENGINE = Memory")
    node2.query("INSERT INTO default.src VALUES (100)")

    node1.query(
        "CREATE TABLE dst ON CLUSTER two_nodes ENGINE = Memory AS SELECT * FROM src",
        database="default.ns",
        settings={"distributed_ddl_output_mode": "null_status_on_timeout"},
    )

    assert node1.query("SELECT sum(x) FROM default.`ns.dst`").strip() == "3"
    assert node2.query("SELECT sum(x) FROM default.`ns.dst`").strip() == "3"

    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS default.`ns.dst`")
        node.query("DROP TABLE IF EXISTS default.`ns.src`")
    node2.query("DROP TABLE IF EXISTS default.src")
