from typing import List

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.test_tools import TSV

from .concurrency_helper import (
    add_nodes_to_cluster,
    create_test_table,
    generate_cluster_def,
)

cluster = ClickHouseCluster(__file__)

# Testing backups.max_attempts_after_bad_version is dynamic, and depends on num_nodes
num_nodes = 20

main_configs = [
    "configs/backups_disk.xml",
    generate_cluster_def(__file__, num_nodes),
]
# No [Zoo]Keeper retries for tests with concurrency
user_configs = ["configs/allow_database_types.xml"]

nodes = add_nodes_to_cluster(cluster, num_nodes, main_configs, user_configs)

node0 = nodes[0]


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
        node0.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")
        node0.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster' SYNC")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def create_and_fill_table() -> None:
    create_test_table(node0)
    for i, node in enumerate(nodes):
        node.query(f"INSERT INTO tbl VALUES ({i})")


expected_sum = num_nodes * (num_nodes - 1) // 2


def test_backup_restore_huge_cluster():
    create_and_fill_table()

    backup_name = new_backup_name()
    node0.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node0.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")
    node0.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node0.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    for i in range(num_nodes):
        assert nodes[i].query("SELECT sum(x) FROM tbl") == TSV([expected_sum])
