import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry, exec_query_with_retry

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/cluster.xml",
    "configs/slow_replicated_merge_tree.xml",
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
        node1.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster' SYNC")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


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
    id, status = node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} ASYNC"
    ).split("\t")

    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["BACKUP_CREATED", ""]]),
    )

    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    id, status = node1.query(
        f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name} ASYNC"
    ).split("\t")

    assert status == "RESTORING\n" or status == "RESTORED\n"

    assert_eq_with_retry(
        node1,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["RESTORED", ""]]),
    )

    # exec_query_with_retry() is here because `SYSTEM SYNC REPLICA` can throw `TABLE_IS_READ_ONLY`
    # if any of these tables didn't start completely yet.
    exec_query_with_retry(node1, "SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")
    exec_query_with_retry(node1, "SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl2")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV([1, 22])
    assert node2.query("SELECT * FROM mydb.tbl2 ORDER BY y") == TSV(["a", "bb"])
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == TSV([1, 22])
    assert node1.query("SELECT * FROM mydb.tbl2 ORDER BY y") == TSV(["a", "bb"])
