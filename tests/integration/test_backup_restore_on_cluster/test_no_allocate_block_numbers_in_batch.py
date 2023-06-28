from time import sleep
import pytest
import re
import os.path
import random, string
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/remote_servers.xml",
    "configs/replicated_access_storage.xml",
    "configs/replicated_user_defined_sql_objects.xml",
    "configs/backups_disk.xml",
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
    "configs/no_allocate_block_numbers_in_batch.xml",
]

user_configs = [
    "configs/allow_database_types.xml",
    "configs/zookeeper_retries.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,  # Necessary for the "test_stop_other_host_while_backup" test
)


node3 = cluster.add_instance(
    "node3",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node3", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP TABLE IF EXISTS tbl2 ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP USER IF EXISTS u1, u2 ON CLUSTER 'cluster3'")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def get_path_to_backup(backup_name):
    name = backup_name.split(",")[1].strip("')/ ")
    return os.path.join(node1.cluster.instances_dir, "backups", name)


def test_replicated_table():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (1, 'Don''t')")
    node2.query("INSERT INTO tbl VALUES (2, 'count')")
    node1.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (3, 'your')")
    node2.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (4, 'chickens')")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=1"
    )

    # Drop table on both nodes.
    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )


def test_empty_replicated_table():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=1"
    )

    # Drop table on both nodes.
    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl") == ""
    assert node2.query("SELECT * FROM tbl") == ""


def test_replicated_database():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("INSERT INTO mydb.tbl VALUES (1, 'Don''t')")
    node2.query("INSERT INTO mydb.tbl VALUES (2, 'count')")
    node1.query("INSERT INTO mydb.tbl VALUES (3, 'your')")
    node2.query("INSERT INTO mydb.tbl VALUES (4, 'chickens')")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    # Make backup.
    backup_name = new_backup_name()
    node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=2"
    )

    # Drop table on both nodes.
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )

    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )


def test_different_tables_on_nodes():
    node1.query(
        "CREATE TABLE tbl (`x` UInt8, `y` String) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("CREATE TABLE tbl (`w` Int64) ENGINE = MergeTree ORDER BY w")

    node1.query(
        "INSERT INTO tbl VALUES (1, 'Don''t'), (2, 'count'), (3, 'your'), (4, 'chickens')"
    )
    node2.query("INSERT INTO tbl VALUES (-333), (-222), (-111), (0), (111)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")

    assert node1.query("SELECT * FROM tbl") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )
    assert node2.query("SELECT * FROM tbl") == TSV([-333, -222, -111, 0, 111])


def test_backup_restore_on_single_replica():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )
    node1.query(
        "CREATE TABLE mydb.test (`name` String, `value` UInt32) ENGINE = ReplicatedMergeTree ORDER BY value"
    )
    node1.query("INSERT INTO mydb.test VALUES ('abc', 1), ('def', 2)")
    node1.query("INSERT INTO mydb.test VALUES ('ghi', 3)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb TO {backup_name}")

    node1.query("DROP DATABASE mydb SYNC")

    # Cannot restore table because it already contains data on other replicas.
    expected_error = "already contains some data"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE DATABASE mydb FROM {backup_name}"
    )

    # Can restore table with structure_only=true.
    node1.query(
        f"RESTORE DATABASE mydb FROM {backup_name} SETTINGS structure_only=true"
    )

    node1.query("SYSTEM SYNC REPLICA mydb.test")
    assert node1.query("SELECT * FROM mydb.test ORDER BY name") == TSV(
        [["abc", 1], ["def", 2], ["ghi", 3]]
    )

    # Can restore table with allow_non_empty_tables=true.
    node1.query("DROP DATABASE mydb SYNC")
    node1.query(
        f"RESTORE DATABASE mydb FROM {backup_name} SETTINGS allow_non_empty_tables=true"
    )

    node1.query("SYSTEM SYNC REPLICA mydb.test")
    assert node1.query("SELECT * FROM mydb.test ORDER BY name") == TSV(
        [["abc", 1], ["abc", 1], ["def", 2], ["def", 2], ["ghi", 3], ["ghi", 3]]
    )


def test_table_with_parts_in_queue_considered_non_empty():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )
    node1.query(
        "CREATE TABLE mydb.test (`x` UInt32) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    node1.query("INSERT INTO mydb.test SELECT number AS x FROM numbers(10000000)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb TO {backup_name}")

    node1.query("DROP DATABASE mydb SYNC")

    # Cannot restore table because it already contains data on other replicas.
    expected_error = "already contains some data"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE DATABASE mydb FROM {backup_name}"
    )


def test_replicated_table_restored_into_bigger_cluster():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster3' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster3' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node3.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_replicated_table_restored_into_smaller_cluster():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster1' FROM {backup_name}")
    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_projection():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' (x UInt32, y String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}') "
        "ORDER BY y PARTITION BY x%10"
    )
    node1.query(f"INSERT INTO tbl SELECT number, toString(number) FROM numbers(3)")

    node1.query("ALTER TABLE tbl ADD PROJECTION prjmax (SELECT MAX(x))")
    node1.query(f"INSERT INTO tbl VALUES (100, 'a'), (101, 'b')")

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "2\n"
    )

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "0\n"
    )

    node1.query(f"RESTORE TABLE tbl FROM {backup_name}")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[0, "0"], [1, "1"], [2, "2"], [100, "a"], [101, "b"]]
    )

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "2\n"
    )
