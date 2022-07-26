from time import sleep
import pytest
import re
import os.path
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/remote_servers.xml",
    "configs/replicated_access_storage.xml",
    "configs/backups_disk.xml",
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
]

user_configs = [
    "configs/allow_database_types.xml",
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
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster3' NO DELAY")
        node1.query("DROP TABLE IF EXISTS tbl2 ON CLUSTER 'cluster3' NO DELAY")
        node1.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster3' NO DELAY")
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
    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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
    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP DATABASE mydb NO DELAY")

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
    node1.query("DROP DATABASE mydb NO DELAY")
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

    node1.query("DROP DATABASE mydb NO DELAY")

    # Cannot restore table because it already contains data on other replicas.
    expected_error = "already contains some data"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE DATABASE mydb FROM {backup_name}"
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

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    backup_name = new_backup_name()
    [id, _, status] = node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} ASYNC"
    ).split("\t")

    assert status == "MAKING_BACKUP\n" or status == "BACKUP_COMPLETE\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE uuid='{id}' AND NOT internal",
        TSV([["BACKUP_COMPLETE", ""]]),
    )

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' NO DELAY")

    [id, _, status] = node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} ASYNC"
    ).split("\t")

    assert status == "RESTORING\n" or status == "RESTORED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE uuid='{id}' AND NOT internal",
        TSV([["RESTORED", ""]]),
    )

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV([1, 22])
    assert node2.query("SELECT * FROM mydb.tbl2 ORDER BY y") == TSV(["a", "bb"])


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

    node1.query("INSERT INTO tbl VALUES (1)")

    backup_name = new_backup_name()

    ids = []
    nodes = [node1, node2]
    on_cluster_part = "ON CLUSTER 'cluster'" if on_cluster else ""
    for node in nodes:
        if interface == "http":
            res = node.http_query(
                f"BACKUP TABLE tbl {on_cluster_part} TO {backup_name} ASYNC"
            )
        else:
            res = node.query(
                f"BACKUP TABLE tbl {on_cluster_part} TO {backup_name} ASYNC"
            )
        ids.append(res.split("\t")[0])

    [id1, id2] = ids

    for i in range(len(nodes)):
        assert_eq_with_retry(
            nodes[i],
            f"SELECT status FROM system.backups WHERE uuid='{ids[i]}' AND status == 'MAKING_BACKUP'",
            "",
        )

    num_completed_backups = sum(
        [
            int(
                nodes[i]
                .query(
                    f"SELECT count() FROM system.backups WHERE uuid='{ids[i]}' AND status == 'BACKUP_COMPLETE' AND NOT internal"
                )
                .strip()
            )
            for i in range(len(nodes))
        ]
    )

    if num_completed_backups != 1:
        for i in range(len(nodes)):
            print(
                nodes[i].query(
                    f"SELECT status, error FROM system.backups WHERE uuid='{ids[i]}' AND NOT internal"
                )
            )

    assert num_completed_backups == 1

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
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
    expected_error = "necessary to have grant BACKUP ON default.tbl"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}", user="u1"
    )

    node1.query("GRANT BACKUP ON tbl TO u1")
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}", user="u1")

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

    expected_error = "necessary to have grant INSERT, CREATE TABLE ON default.tbl2"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    node1.query("GRANT INSERT, CREATE TABLE ON tbl2 TO u1")
    node1.query(
        f"RESTORE TABLE tbl AS tbl2 ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    assert node2.query("SELECT * FROM tbl2") == "100\n"

    node1.query(f"DROP TABLE tbl2 ON CLUSTER 'cluster' NO DELAY")
    node1.query("REVOKE ALL FROM u1")

    expected_error = "necessary to have grant INSERT, CREATE TABLE ON default.tbl"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE ALL ON CLUSTER 'cluster' FROM {backup_name}", user="u1"
    )

    node1.query("GRANT INSERT, CREATE TABLE ON tbl TO u1")
    node1.query(f"RESTORE ALL ON CLUSTER 'cluster' FROM {backup_name}", user="u1")

    assert node2.query("SELECT * FROM tbl") == "100\n"


def test_system_users():
    node1.query("CREATE USER u1 SETTINGS custom_a=123")
    node1.query("GRANT SELECT ON tbl TO u1")

    backup_name = new_backup_name()
    node1.query("CREATE USER u2 SETTINGS allow_backup=false")
    node1.query("GRANT CLUSTER ON *.* TO u2")

    expected_error = "necessary to have grant BACKUP ON system.users"
    assert expected_error in node1.query_and_get_error(
        f"BACKUP TABLE system.users ON CLUSTER 'cluster' TO {backup_name}", user="u2"
    )

    node1.query("GRANT BACKUP ON system.users TO u2")
    node1.query(
        f"BACKUP TABLE system.users ON CLUSTER 'cluster' TO {backup_name}", user="u2"
    )

    node1.query("DROP USER u1")

    expected_error = "necessary to have grant CREATE USER ON *.*"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    node1.query("GRANT CREATE USER ON *.* TO u2")

    expected_error = "necessary to have grant SELECT ON default.tbl WITH GRANT OPTION"
    assert expected_error in node1.query_and_get_error(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    node1.query("GRANT SELECT ON tbl TO u2 WITH GRANT OPTION")
    node1.query(
        f"RESTORE TABLE system.users ON CLUSTER 'cluster' FROM {backup_name}", user="u2"
    )

    assert (
        node1.query("SHOW CREATE USER u1") == "CREATE USER u1 SETTINGS custom_a = 123\n"
    )
    assert node1.query("SHOW GRANTS FOR u1") == "GRANT SELECT ON default.tbl TO u1\n"


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

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' NO DELAY")

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

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' NO DELAY")

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

    assert not has_mutation_in_backup("0000000000", backup_name, "default", "tbl")
    assert has_mutation_in_backup("0000000001", backup_name, "default", "tbl")
    assert has_mutation_in_backup("0000000002", backup_name, "default", "tbl")
    assert not has_mutation_in_backup("0000000003", backup_name, "default", "tbl")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")


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
        f"SELECT status FROM system.backups WHERE uuid='{id}' AND status == 'MAKING_BACKUP' AND NOT internal",
        "",
        retry_count=100,
    )

    status = node1.query(
        f"SELECT status FROM system.backups WHERE uuid='{id}' AND NOT internal"
    ).strip()

    if kill:
        assert status in ["BACKUP_COMPLETE", "FAILED_TO_BACKUP"]
    else:
        assert status == "BACKUP_COMPLETE"

    node2.start_clickhouse()

    if status == "BACKUP_COMPLETE":
        node1.query("DROP TABLE tbl ON CLUSTER 'cluster' NO DELAY")
        node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
        assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([3, 5])
    elif status == "FAILED_TO_BACKUP":
        assert not os.path.exists(
            os.path.join(get_path_to_backup(backup_name), ".backup")
        )
