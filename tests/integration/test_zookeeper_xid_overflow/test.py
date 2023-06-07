# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    base_config_dir="configs",
    main_configs=[
        "configs/config.d/text_log.xml",
        "configs/config.d/zookeeper.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_zookeeper_xid_overflow(started_cluster):
    node.query("SET log_query_threads = 1")
    node.query("SELECT count() FROM system.tables")
    node.query("SYSTEM FLUSH LOGS")

    # Create a table.
    node.query(
        "CREATE TABLE t1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/t1', 'node') ORDER BY x" 
    )

    # Insert 10 rows.
    node.query("INSERT INTO t1 VALUES(1)")
    node.query("INSERT INTO t1 VALUES(2)")
    node.query("INSERT INTO t1 VALUES(3)")
    node.query("INSERT INTO t1 VALUES(4)")
    node.query("INSERT INTO t1 VALUES(5)")
    node.query("INSERT INTO t1 VALUES(6)")
    node.query("INSERT INTO t1 VALUES(7)")
    node.query("INSERT INTO t1 VALUES(8)")
    node.query("INSERT INTO t1 VALUES(9)")
    node.query("INSERT INTO t1 VALUES(10)")
    node.query("INSERT INTO t1 VALUES(11)")
    node.query("INSERT INTO t1 VALUES(12)")
    node.query("INSERT INTO t1 VALUES(13)")
    node.query("INSERT INTO t1 VALUES(14)")
    node.query("SYSTEM FLUSH LOGS")

    # There is at least one record of an xid overflow in the system.text_log.
    result1 = node.query("SELECT count() FROM system.text_log WHERE message LIKE 'Reset the XID to % to avoid session expiration'")
    print("Result1: {result1}")
    assert(int(result1) >= 1)

    # There should be no warning or error messages. 
    result2 = node.query("SELECT count() FROM system.text_log WHERE level IN ('Warning', 'Error')")
    print("Result2: {result2}")
    assert(int(result2) == 0)

