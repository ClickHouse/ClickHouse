import time
import logging

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance('main_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True, stay_alive=True)
dummy_node = cluster.add_instance('dummy_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True)
competing_node = cluster.add_instance('competing_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True)
snapshotting_node = cluster.add_instance('snapshotting_node', main_configs=['configs/snapshot_each_query.xml'], with_zookeeper=True)
snapshot_recovering_node = cluster.add_instance('snapshot_recovering_node', main_configs=['configs/disable_snapshots.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        main_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica1');")
        dummy_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica2');")
        yield cluster

    finally:
        cluster.shutdown()


def test_create_replicated_table(started_cluster):
    DURATION_SECONDS = 1
    main_node.query("CREATE TABLE testdb.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree(d, k, 8192);")

    time.sleep(DURATION_SECONDS)
    assert main_node.query("desc table testdb.replicated_table") == dummy_node.query("desc table testdb.replicated_table")

def test_simple_alter_table(started_cluster):
    DURATION_SECONDS = 1
    main_node.query("CREATE TABLE testdb.alter_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added0 UInt32;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added2 UInt32;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added1 UInt32 AFTER Added0;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;")

    time.sleep(DURATION_SECONDS)
    assert main_node.query("desc table testdb.alter_test") == dummy_node.query("desc table testdb.alter_test")

def test_create_replica_after_delay(started_cluster):
    competing_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica3');")

    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added3 UInt32 ;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added4 UInt32 ;")
    main_node.query("ALTER TABLE testdb.alter_test ADD COLUMN Added5 UInt32 ;")

    time.sleep(6)

    assert competing_node.query("desc table testdb.alter_test") == main_node.query("desc table testdb.alter_test")

def test_alters_from_different_replicas(started_cluster):
    DURATION_SECONDS = 1

    main_node.query("CREATE TABLE testdb.concurrent_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);")

    time.sleep(DURATION_SECONDS)

    competing_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN Added0 UInt32;")
    main_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN Added2 UInt32;")
    competing_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN Added1 UInt32 AFTER Added0;")
    main_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested1 Nested(A UInt32, B UInt64) AFTER Added2;")
    competing_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested1.C Array(String) AFTER AddedNested1.B;")
    main_node.query("ALTER TABLE testdb.concurrent_test ADD COLUMN AddedNested2 Nested(A UInt32, B UInt64) AFTER AddedNested1;")

    time.sleep(DURATION_SECONDS)

    assert competing_node.query("desc table testdb.concurrent_test") == main_node.query("desc table testdb.concurrent_test")

def test_drop_and_create_table(started_cluster):
    main_node.query("DROP TABLE testdb.concurrent_test")
    main_node.query("CREATE TABLE testdb.concurrent_test (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192);")
    time.sleep(5)
    assert competing_node.query("desc table testdb.concurrent_test") == main_node.query("desc table testdb.concurrent_test")

def test_replica_restart(started_cluster):
    main_node.restart_clickhouse()
    time.sleep(5)
    assert competing_node.query("desc table testdb.concurrent_test") == main_node.query("desc table testdb.concurrent_test")

def test_snapshot_and_snapshot_recover(started_cluster):
    snapshotting_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica4');")
    time.sleep(5)
    snapshot_recovering_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica5');")
    time.sleep(5)
    assert snapshotting_node.query("desc table testdb.alter_test") == snapshot_recovering_node.query("desc table testdb.alter_test")

#def test_drop_and_create_replica(started_cluster):
#    main_node.query("DROP DATABASE testdb")
#    main_node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', 'replica1');")
#    time.sleep(6)
#    assert competing_node.query("desc table testdb.concurrent_test") == main_node.query("desc table testdb.concurrent_test")

