import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/cluster.xml",
    "configs/backups_disk.xml",
]

user_configs = []

image = "clickhouse/integration-test"
tag = None

#image = "clickhouse/clickhouse-server"
#tag = "24.10"

node1 = cluster.add_instance(
    "node1",
    image=image,
    tag=tag,
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    image=image,
    tag=tag,
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
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def test_really_many_tables():
    num_tables = 500

    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    for i in range(1, num_tables):
        node1.query(
            f"CREATE TABLE mydb.tbl{i}(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
        )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    # Make backup.
    backup_name = new_backup_name()
    old_time = time.monotonic()
    node1.query(f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name}")
    backup_time = time.monotonic() - old_time

    print(f"Making backup of {num_tables} tables took {backup_time} seconds")

    # Drop table on both nodes.
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # Restore from backup.
    old_time = time.monotonic()
    node1.query(f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name}")
    restore_time = time.monotonic() - old_time

    print(f"Restoring of {num_tables} tables took {restore_time} seconds")

    assert False
