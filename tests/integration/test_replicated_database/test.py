import time
import re
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance('main_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True, stay_alive=True, macros={"shard": 1, "replica": 1})
dummy_node = cluster.add_instance('dummy_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True, macros={"shard": 1, "replica": 2})
competing_node = cluster.add_instance('competing_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True, macros={"shard": 1, "replica": 3})
snapshotting_node = cluster.add_instance('snapshotting_node', main_configs=['configs/snapshot_each_query.xml'], with_zookeeper=True, macros={"shard": 2, "replica": 1})
snapshot_recovering_node = cluster.add_instance('snapshot_recovering_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True, macros={"shard": 2, "replica": 2})

uuid_regex = re.compile("[0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12}")
def assert_create_query(nodes, table_name, expected):
    replace_uuid = lambda x: re.sub(uuid_regex, "uuid", x)
    query = "show create table testdb.{}".format(table_name)
    for node in nodes:
        assert_eq_with_retry(node, query, expected, get_result=replace_uuid)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        main_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica1');")
        dummy_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica2');")
        yield cluster

    finally:
        cluster.shutdown()

#TODO better tests

def test_create_replicated_table(started_cluster):
    #FIXME should fail (replicated with old syntax)
    #main_node.query("CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree(d, k, 8192);")
    main_node.query("CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree ORDER BY k PARTITION BY toYYYYMM(d);")

    expected = "CREATE TABLE testdb.replicated_table\\n(\\n    `d` Date,\\n    `k` UInt64,\\n    `i32` Int32\\n)\\n" \
               "ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/uuid/{shard}\\', \\'{replica}\\')\\n" \
               "PARTITION BY toYYYYMM(d)\\nORDER BY k\\nSETTINGS index_granularity = 8192"
    assert_create_query([main_node, dummy_node], "replicated_table", expected)
    # assert without replacing uuid
    assert main_node.query("show create testdb.replicated_table") == dummy_node.query("show create testdb.replicated_table")

def test_simple_alter_table(started_cluster):
    #TODO add test with ReplicatedMergeTree
    main_node.query("CREATE TABLE testdb.alter_test "
                    "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
                    "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added0 UInt32;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added2 UInt32;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added1 UInt32 AFTER Added0;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;")

    expected = "CREATE TABLE testdb.alter_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n" \
               "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n" \
               "    `ToDrop` UInt32,\\n    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n" \
               "    `AddedNested1.A` Array(UInt32),\\n    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n" \
               "    `AddedNested2.A` Array(UInt32),\\n    `AddedNested2.B` Array(UInt64)\\n)\\n" \
               "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

    assert_create_query([main_node, dummy_node], "alter_test", expected)

def test_create_replica_after_delay(started_cluster):
    competing_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica3');")

    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added3 UInt32;")
    main_node.query("ALTER TABLE testdb.alter_test DROP COLUMN AddedNested1;")
    main_node.query("ALTER TABLE testdb.alter_test RENAME COLUMN Added1 TO AddedNested1;")

    expected = "CREATE TABLE testdb.alter_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n" \
               "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n" \
               "    `ToDrop` UInt32,\\n    `Added0` UInt32,\\n    `AddedNested1` UInt32,\\n    `Added2` UInt32,\\n" \
               "    `AddedNested2.A` Array(UInt32),\\n    `AddedNested2.B` Array(UInt64),\\n    `Added3` UInt32\\n)\\n" \
               "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

    assert_create_query([main_node, dummy_node, competing_node], "alter_test", expected)

def test_alters_from_different_replicas(started_cluster):
    main_node.query("CREATE TABLE testdb.concurrent_test "
                    "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
                    "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);")

    time.sleep(1)   #FIXME
    dummy_node.kill_clickhouse(stop_start_wait_sec=0)

    competing_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN Added0 UInt32;")
    main_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN Added2 UInt32;")
    competing_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN Added1 UInt32 AFTER Added0;")
    main_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;")
    competing_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;")
    main_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;")

    expected = "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n" \
               "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32,\\n" \
               "    `Added0` UInt32,\\n    `Added1` UInt32,\\n    `Added2` UInt32,\\n    `AddedNested1.A` Array(UInt32),\\n" \
               "    `AddedNested1.B` Array(UInt64),\\n    `AddedNested1.C` Array(String),\\n    `AddedNested2.A` Array(UInt32),\\n" \
               "    `AddedNested2.B` Array(UInt64)\\n)\\n" \
               "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

    assert_create_query([main_node, competing_node], "concurrent_test", expected)

def test_drop_and_create_table(started_cluster):
    main_node.query("DROP TABLE testdb.concurrent_test")
    main_node.query("CREATE TABLE testdb.concurrent_test "
                    "(CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) "
                    "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);")

    expected = "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n" \
               "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n" \
               "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

    assert_create_query([main_node, competing_node], "concurrent_test", expected)

def test_replica_restart(started_cluster):
    main_node.restart_clickhouse()

    expected = "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n" \
               "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n" \
               "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

    assert_create_query([main_node, competing_node], "concurrent_test", expected)

def test_snapshot_and_snapshot_recover(started_cluster):
    #FIXME bad test
    snapshotting_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica4');")
    time.sleep(5)
    snapshot_recovering_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica5');")
    time.sleep(5)
    assert snapshotting_node.query("desc table testdb.alter_test") == snapshot_recovering_node.query("desc table testdb.alter_test")

def test_drop_and_create_replica(started_cluster):
    main_node.query("DROP DATABASE testdb")
    main_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica1');")

    expected = "CREATE TABLE testdb.concurrent_test\\n(\\n    `CounterID` UInt32,\\n    `StartDate` Date,\\n    `UserID` UInt32,\\n" \
               "    `VisitID` UInt32,\\n    `NestedColumn.A` Array(UInt8),\\n    `NestedColumn.S` Array(String),\\n    `ToDrop` UInt32\\n)\\n" \
               "ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

    assert_create_query([main_node, competing_node], "concurrent_test", expected)

#TODO tests with Distributed

