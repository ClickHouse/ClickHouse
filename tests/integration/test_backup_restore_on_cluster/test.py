import os.path
import random
import re
import string

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/cluster.xml",
    "configs/cluster3.xml",
    "configs/replicated_access_storage.xml",
    "configs/replicated_user_defined_sql_objects.xml",
    "configs/backups_disk.xml",
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
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
        node1.query("DROP DATABASE IF EXISTS mydb2 ON CLUSTER 'cluster3' SYNC")
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
    node1.query("OPTIMIZE TABLE mydb.tbl ON CLUSTER 'cluster' FINAL")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    # check data in sync
    expect = TSV([[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]])
    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == expect
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == expect

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

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == expect
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == expect


def test_replicated_database_compare_parts():
    """
    stop merges and fetches then write data to two nodes and
    compare that parts are restored from single node (second) after backup
    replica is selected by settings replica_num=2, replica_num_in_backup=2
    """
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("SYSTEM STOP MERGES mydb.tbl")
    node2.query("SYSTEM STOP MERGES mydb.tbl")

    node1.query("SYSTEM STOP FETCHES mydb.tbl")
    node2.query("SYSTEM STOP FETCHES mydb.tbl")

    node1.query("INSERT INTO mydb.tbl VALUES (1, 'a')")
    node1.query("INSERT INTO mydb.tbl VALUES (2, 'b')")

    node2.query("INSERT INTO mydb.tbl VALUES (3, 'x')")
    node2.query("INSERT INTO mydb.tbl VALUES (4, 'y')")

    p2 = node2.query("SELECT * FROM mydb.tbl ORDER BY x")

    # Make backup.
    backup_name = new_backup_name()
    node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=2"
    )

    # Drop table on both nodes.
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=2"
    )
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    # compare parts
    p1_ = node1.query("SELECT _part, * FROM mydb.tbl ORDER BY x")
    p2_ = node2.query("SELECT _part, * FROM mydb.tbl ORDER BY x")
    assert p1_ == p2_

    # compare data
    assert p2 == node2.query("SELECT * FROM mydb.tbl ORDER BY x")


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


def test_replicated_table_with_uuid_in_zkpath():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}','{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (1, 'AA')")
    node2.query("INSERT INTO tbl VALUES (2, 'BB')")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    # The table `tbl2` is expected to have a different UUID so it's ok to have both `tbl` and `tbl2` at the same time.
    node2.query(f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}")

    node1.query("INSERT INTO tbl2 VALUES (3, 'CC')")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    for instance in [node1, node2]:
        assert instance.query("SELECT * FROM tbl ORDER BY x") == TSV(
            [[1, "AA"], [2, "BB"]]
        )
        assert instance.query("SELECT * FROM tbl2 ORDER BY x") == TSV(
            [[1, "AA"], [2, "BB"], [3, "CC"]]
        )


def test_replicated_table_with_not_synced_insert():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM STOP REPLICATED SENDS ON CLUSTER 'cluster' tbl")

    node1.query("INSERT INTO tbl VALUES (333)")
    node2.query("INSERT INTO tbl VALUES (444)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])


def test_replicated_table_with_not_synced_merge():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("SYSTEM STOP MERGES ON CLUSTER 'cluster' tbl")

    node1.query("INSERT INTO tbl VALUES (111)")
    node1.query("INSERT INTO tbl VALUES (222)")

    node2.query("SYSTEM SYNC REPLICA tbl")

    node2.query("SYSTEM START MERGES tbl")
    node2.query("OPTIMIZE TABLE tbl FINAL")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


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


def test_replicated_database_async():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query("CREATE TABLE mydb.tbl(x UInt8) ENGINE=ReplicatedMergeTree ORDER BY x")

    node1.query(
        "CREATE TABLE mydb.tbl2(y String) ENGINE=ReplicatedMergeTree ORDER BY y"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("INSERT INTO mydb.tbl VALUES (1)")
    node1.query("INSERT INTO mydb.tbl VALUES (22)")
    node2.query("INSERT INTO mydb.tbl2 VALUES ('a')")
    node2.query("INSERT INTO mydb.tbl2 VALUES ('bb')")

    node1.query("OPTIMIZE TABLE mydb.tbl ON CLUSTER 'cluster' FINAL")
    node1.query("OPTIMIZE TABLE mydb.tbl2 ON CLUSTER 'cluster' FINAL")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl2")

    backup_name = new_backup_name()
    [id, status] = node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} ASYNC"
    ).split("\t")

    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["BACKUP_CREATED", ""]]),
    )

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    [id, status] = node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} ASYNC"
    ).split("\t")

    assert status == "RESTORING\n" or status == "RESTORED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["RESTORED", ""]]),
    )

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl2")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV([1, 22])
    assert node2.query("SELECT * FROM mydb.tbl2 ORDER BY y") == TSV(["a", "bb"])


@pytest.mark.parametrize("special_macro", ["uuid", "database"])
def test_replicated_database_with_special_macro_in_zk_path(special_macro):
    zk_path = "/clickhouse/databases/{" + special_macro + "}"
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('"
        + zk_path
        + "','{shard}','{replica}')"
    )

    # ReplicatedMergeTree without arguments means ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
    node1.query("CREATE TABLE mydb.tbl(x Int64) ENGINE=ReplicatedMergeTree ORDER BY x")

    node1.query("INSERT INTO mydb.tbl VALUES (-3)")
    node1.query("INSERT INTO mydb.tbl VALUES (1)")
    node1.query("INSERT INTO mydb.tbl VALUES (10)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name}")

    # RESTORE DATABASE with rename should work here because the new database will have another UUID and thus another zookeeper path.
    node1.query(
        f"RESTORE DATABASE mydb AS mydb2 ON CLUSTER 'cluster' FROM {backup_name}"
    )

    node1.query("INSERT INTO mydb.tbl VALUES (2)")

    node1.query("SYSTEM SYNC DATABASE REPLICA ON CLUSTER 'cluster' mydb2")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb2.tbl")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV(
        [[-3], [1], [2], [10]]
    )

    assert node1.query("SELECT * FROM mydb2.tbl ORDER BY x") == TSV([[-3], [1], [10]])
    assert node2.query("SELECT * FROM mydb2.tbl ORDER BY x") == TSV([[-3], [1], [10]])


# By default `backup_restore_keeper_value_max_size` is 1 MB, but in this test we'll set it to 50 bytes just to check it works.
def test_keeper_value_max_size():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM STOP REPLICATED SENDS ON CLUSTER 'cluster' tbl")

    node1.query("INSERT INTO tbl VALUES (333)")
    node2.query("INSERT INTO tbl VALUES (444)")

    backup_name = new_backup_name()
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}",
        settings={"backup_restore_keeper_value_max_size": 50},
    )

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222, 333, 444])


@pytest.mark.parametrize(
    "interface, on_cluster", [("native", True), ("http", True), ("http", False)]
)
def test_async_backups_to_same_destination(interface, on_cluster):
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    nodes = [node1, node2]
    node1.query("INSERT INTO tbl VALUES (1)")

    backup_name = new_backup_name()
    on_cluster_part = "ON CLUSTER 'cluster'" if on_cluster else ""

    # Multiple backups to the same destination.
    ids = []
    for node in nodes:
        if interface == "http":
            res, err = node.http_query_and_get_answer_with_error(
                f"BACKUP TABLE tbl {on_cluster_part} TO {backup_name} ASYNC"
            )
        else:
            res, err = node.query_and_get_answer_with_error(
                f"BACKUP TABLE tbl {on_cluster_part} TO {backup_name} ASYNC"
            )

        # The second backup to the same destination is expected to fail. It can either fail immediately or after a while.
        # If it fails immediately we won't even get its ID.
        if not err:
            ids.append(res.split("\t")[0])

    ids_for_query = "[" + ", ".join(f"'{id}'" for id in ids) + "]"

    for node in nodes:
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.backups WHERE id IN {ids_for_query} AND status == 'CREATING_BACKUP'",
            "",
        )

    num_created_backups = sum(
        [
            int(
                node.query(
                    f"SELECT count() FROM system.backups WHERE id IN {ids_for_query} AND status == 'BACKUP_CREATED'"
                ).strip()
            )
            for node in nodes
        ]
    )

    num_failed_backups = sum(
        [
            int(
                node.query(
                    f"SELECT count() FROM system.backups WHERE id IN {ids_for_query} AND status == 'BACKUP_FAILED'"
                ).strip()
            )
            for node in nodes
        ]
    )

    # Only one backup should succeed.
    if (num_created_backups != 1) or (num_failed_backups != len(ids) - 1):
        for node in nodes:
            print(
                node.query(
                    f"SELECT status, error FROM system.backups WHERE id IN {ids_for_query}"
                )
            )

    assert num_created_backups == 1
    assert num_failed_backups == len(ids) - 1

    # Check that the succeeded backup is all right.
    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node1.query(f"RESTORE TABLE tbl FROM {backup_name}")
    assert node1.query("SELECT * FROM tbl") == "1\n"


def test_required_privileges():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (100)")

    node1.query("CREATE USER u1")
    node1.query("GRANT CLUSTER ON *.* TO u1")

    backup_name = new_backup_name()
    expected_error = "necessary to have the grant BACKUP ON default.tbl"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}", user="u1"
    )

    node1.query("GRANT BACKUP ON tbl TO u1")
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}", user="u1")

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON default.tbl2"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    node1.query("GRANT INSERT, CREATE TABLE ON tbl2 TO u1")
    node1.query(
        f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )
    node2.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    assert node2.query("SELECT * FROM tbl2") == "100\n"

    node1.query(f"DROP TABLE tbl2 ON CLUSTER 'cluster' SYNC")
    node1.query("REVOKE ALL FROM u1")

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON default.tbl"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE ALL ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    node1.query("GRANT INSERT, CREATE TABLE ON tbl TO u1")
    node1.query(f"RESTORE ALL ON CLUSTER 'cluster' FROM {backup_name}", user="u1")
    node2.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node2.query("SELECT * FROM tbl") == "100\n"


def test_system_users():
    node1.query("CREATE USER u1 SETTINGS custom_a=123")
    node1.query("GRANT SELECT ON tbl TO u1")

    backup_name = new_backup_name()
    node1.query("CREATE USER u2 SETTINGS allow_backup=false")
    node1.query("GRANT CLUSTER ON *.* TO u2")

    expected_error = "necessary to have the grant BACKUP ON system.users"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE system.users ON CLUSTER 'cluster' TO {backup_name}", user="u2"
    )

    node1.query("GRANT BACKUP ON system.users TO u2")
    node1.query(
        f"BACKUP TABLE system.users ON CLUSTER 'cluster' TO {backup_name}", user="u2"
    )

    node1.query("DROP USER u1")

    expected_error = "necessary to have the grant CREATE USER ON *.*"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    node1.query("GRANT CREATE USER ON *.* TO u2")

    expected_error = (
        "necessary to have the grant SELECT ON default.tbl WITH GRANT OPTION"
    )
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    node1.query("GRANT SELECT ON tbl TO u2 WITH GRANT OPTION")
    node1.query(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    assert (
        node1.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH no_password SETTINGS custom_a = 123\n"
    )
    assert node1.query("SHOW GRANTS FOR u1") == "GRANT SELECT ON default.tbl TO u1\n"


def test_system_functions():
    node1.query("CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;")
    node1.query("CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE system.functions ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP FUNCTION linear_equation")
    node1.query("DROP FUNCTION parity_str")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.functions WHERE name='parity_str'", ""
    )

    node1.query(
        f"RESTORE TABLE system.functions ON CLUSTER 'cluster' FROM {backup_name}"
    )

    assert node1.query(
        "SELECT number, linear_equation(number, 2, 1) FROM numbers(3)"
    ) == TSV([[0, 1], [1, 3], [2, 5]])

    assert node1.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )

    assert node2.query(
        "SELECT number, linear_equation(number, 2, 1) FROM numbers(3)"
    ) == TSV([[0, 1], [1, 3], [2, 5]])

    assert node2.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )

    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.functions WHERE name='parity_str'",
        "parity_str\n",
    )
    assert node2.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )

    node1.query("DROP FUNCTION linear_equation")
    node1.query("DROP FUNCTION parity_str")


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


def test_file_deduplication():
    # Random column name helps finding it in logs.
    column_name = "".join(random.choice(string.ascii_letters) for x in range(10))

    # Make four replicas in total: 2 on each host.
    node1.query(
        f"""
        CREATE TABLE tbl ON CLUSTER 'cluster' (
        {column_name} Int32
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{{replica}}')
        ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0
        """
    )

    node1.query(
        f"""
        CREATE TABLE tbl2 ON CLUSTER 'cluster' (
        {column_name} Int32
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{{replica}}-2')
        ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0
        """
    )

    # Unique data.
    node1.query(
        f"INSERT INTO tbl VALUES (3556), (1177), (4004), (4264), (3729), (1438), (2158), (2684), (415), (1917)"
    )
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl, TABLE tbl2 ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("SYSTEM FLUSH LOGS ON CLUSTER 'cluster'")

    # The bin file should be written to the backup once, and skipped three times (because there are four replicas in total).
    bin_file_writing_log_line = (
        f"Writing backup for file .*{column_name}.bin .* (disk default)"
    )
    bin_file_skip_log_line = f"Writing backup for file .*{column_name}.bin .* skipped"

    num_bin_file_writings = int(node1.count_in_log(bin_file_writing_log_line)) + int(
        node2.count_in_log(bin_file_writing_log_line)
    )
    num_bin_file_skips = int(node1.count_in_log(bin_file_skip_log_line)) + int(
        node2.count_in_log(bin_file_skip_log_line)
    )

    assert num_bin_file_writings == 1
    assert num_bin_file_skips == 3


def test_replicated_table_with_not_synced_def():
    node1.query(
        "CREATE TABLE tbl ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY tuple()"
    )

    node2.query(
        "CREATE TABLE tbl ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY tuple()"
    )

    node2.query("SYSTEM STOP REPLICATION QUEUES tbl")
    node1.query("ALTER TABLE tbl MODIFY COLUMN x String")

    # Not synced because the replication queue is stopped
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "UInt8"], ["y", "String"]])

    backup_name = new_backup_name()
    node2.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # But synced after RESTORE anyway
    node1.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=1"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node2.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=2"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='default' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])


def test_table_in_replicated_database_with_not_synced_def():
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY tuple()"
    )

    node1.query("ALTER TABLE mydb.tbl MODIFY COLUMN x String")

    backup_name = new_backup_name()
    node2.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # But synced after RESTORE anyway
    node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=1"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    node2.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} SETTINGS replica_num_in_backup=2"
    )
    assert node1.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])
    assert node2.query(
        "SELECT name, type FROM system.columns WHERE database='mydb' AND table='tbl'"
    ) == TSV([["x", "String"], ["y", "String"]])


def has_mutation_in_backup(mutation_id, backup_name, database, table):
    return (
        os.path.exists(
            os.path.join(
                get_path_to_backup(backup_name),
                f"shards/1/replicas/1/data/{database}/{table}/mutations/{mutation_id}.txt",
            )
        )
        or os.path.exists(
            os.path.join(
                get_path_to_backup(backup_name),
                f"shards/1/replicas/2/data/{database}/{table}/mutations/{mutation_id}.txt",
            )
        )
        or os.path.exists(
            os.path.join(
                get_path_to_backup(backup_name),
                f"shards/1/replicas/3/data/{database}/{table}/mutations/{mutation_id}.txt",
            )
        )
    )


def test_mutation():
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY tuple()"
    )

    node1.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(5)")

    node2.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(5, 5)")

    node1.query("INSERT INTO tbl SELECT number, toString(number) FROM numbers(10, 5)")

    node1.query("ALTER TABLE tbl UPDATE x=x+1 WHERE 1")
    node1.query("ALTER TABLE tbl UPDATE x=x+1+sleep(3) WHERE 1")
    node1.query("ALTER TABLE tbl UPDATE x=x+1+sleep(3) WHERE 1")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    # mutation #0000000000: "UPDATE x=x+1 WHERE 1" could already finish before starting the backup
    # mutation #0000000001: "UPDATE x=x+1+sleep(3) WHERE 1"
    assert has_mutation_in_backup("0000000001", backup_name, "default", "tbl")
    # mutation #0000000002: "UPDATE x=x+1+sleep(3) WHERE 1"
    assert has_mutation_in_backup("0000000002", backup_name, "default", "tbl")
    # mutation #0000000003: not expected
    assert not has_mutation_in_backup("0000000003", backup_name, "default", "tbl")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")


def test_tables_dependency():
    node1.query("CREATE DATABASE mydb ON CLUSTER 'cluster3'")

    node1.query(
        "CREATE TABLE mydb.src ON CLUSTER 'cluster' (x Int64, y String) ENGINE=MergeTree ORDER BY tuple()"
    )

    node1.query(
        "CREATE DICTIONARY mydb.dict ON CLUSTER 'cluster' (x Int64, y String) PRIMARY KEY x "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB 'mydb' TABLE 'src')) LAYOUT(FLAT()) LIFETIME(0)"
    )

    node1.query(
        "CREATE TABLE mydb.dist1 (x Int64) ENGINE=Distributed('cluster', 'mydb', 'src')"
    )

    node3.query(
        "CREATE TABLE mydb.dist2 (x Int64) ENGINE=Distributed(cluster, 'mydb', 'src')"
    )

    node1.query("CREATE TABLE mydb.clusterfunc1 AS cluster('cluster', 'mydb.src')")
    node1.query("CREATE TABLE mydb.clusterfunc2 AS cluster(cluster, mydb.src)")
    node1.query("CREATE TABLE mydb.clusterfunc3 AS cluster(cluster, 'mydb', 'src')")
    node1.query(
        "CREATE TABLE mydb.clusterfunc4 AS cluster(cluster, dictionary(mydb.dict))"
    )
    node1.query(
        "CREATE TABLE mydb.clusterfunc5 AS clusterAllReplicas(cluster, dictionary(mydb.dict))"
    )

    node3.query("CREATE TABLE mydb.clusterfunc6 AS cluster('cluster', 'mydb.src')")
    node3.query("CREATE TABLE mydb.clusterfunc7 AS cluster(cluster, mydb.src)")
    node3.query("CREATE TABLE mydb.clusterfunc8 AS cluster(cluster, 'mydb', 'src')")
    node3.query(
        "CREATE TABLE mydb.clusterfunc9 AS cluster(cluster, dictionary(mydb.dict))"
    )
    node3.query(
        "CREATE TABLE mydb.clusterfunc10 AS clusterAllReplicas(cluster, dictionary(mydb.dict))"
    )

    backup_name = new_backup_name()
    node3.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster3' TO {backup_name}")

    node3.query("DROP DATABASE mydb")

    node3.query(f"RESTORE DATABASE mydb ON CLUSTER 'cluster3' FROM {backup_name}")

    node3.query("SYSTEM FLUSH LOGS ON CLUSTER 'cluster3'")
    expect_in_logs_1 = [
        "Table mydb.src has no dependencies (level 0)",
        "Table mydb.dict has 1 dependencies: mydb.src (level 1)",
        "Table mydb.dist1 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc1 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc2 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc3 has 1 dependencies: mydb.src (level 1)",
        "Table mydb.clusterfunc4 has 1 dependencies: mydb.dict (level 2)",
        "Table mydb.clusterfunc5 has 1 dependencies: mydb.dict (level 2)",
    ]
    expect_in_logs_2 = [
        "Table mydb.src has no dependencies (level 0)",
        "Table mydb.dict has 1 dependencies: mydb.src (level 1)",
    ]
    expect_in_logs_3 = [
        "Table mydb.dist2 has no dependencies (level 0)",
        "Table mydb.clusterfunc6 has no dependencies (level 0)",
        "Table mydb.clusterfunc7 has no dependencies (level 0)",
        "Table mydb.clusterfunc8 has no dependencies (level 0)",
        "Table mydb.clusterfunc9 has no dependencies (level 0)",
        "Table mydb.clusterfunc10 has no dependencies (level 0)",
    ]
    for expect in expect_in_logs_1:
        assert node1.contains_in_log(f"RestorerFromBackup: {expect}")
    for expect in expect_in_logs_2:
        assert node2.contains_in_log(f"RestorerFromBackup: {expect}")
    for expect in expect_in_logs_3:
        assert node3.contains_in_log(f"RestorerFromBackup: {expect}")


def test_get_error_from_other_host():
    node1.query("CREATE TABLE tbl (`x` UInt8) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO tbl VALUES (3)")

    backup_name = new_backup_name()
    expected_error = "Got error from node2.*Table default.tbl was not found"
    assert re.search(
        expected_error,
        node1.query_and_get_error(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}"
        ),
    )


@pytest.mark.parametrize("kill", [False, True])
def test_stop_other_host_during_backup(kill):
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (3)")
    node2.query("INSERT INTO tbl VALUES (5)")

    backup_name = new_backup_name()

    id = node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC"
    ).split("\t")[0]

    # If kill=False the pending backup must be completed
    # If kill=True the pending backup might be completed or failed
    node2.stop_clickhouse(kill=kill)

    assert_eq_with_retry(
        node1,
        f"SELECT status FROM system.backups WHERE id='{id}' AND status == 'CREATING_BACKUP'",
        "",
        retry_count=100,
    )

    status = node1.query(f"SELECT status FROM system.backups WHERE id='{id}'").strip()

    if kill:
        expected_statuses = ["BACKUP_CREATED", "BACKUP_FAILED"]
    else:
        expected_statuses = ["BACKUP_CREATED", "BACKUP_CANCELLED"]

    assert status in expected_statuses

    node2.start_clickhouse()

    if status == "BACKUP_CREATED":
        node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
        node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
        node1.query("SYSTEM SYNC REPLICA tbl")
        assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([3, 5])
    elif status == "BACKUP_FAILED":
        assert not os.path.exists(
            os.path.join(get_path_to_backup(backup_name), ".backup")
        )
