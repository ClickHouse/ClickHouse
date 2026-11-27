import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/cluster_2x2.xml",
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
]

user_configs = [
    "configs/zookeeper_retries.xml",
]

node_1_1 = cluster.add_instance(
    "node_1_1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "1", "shard": "1"},
    with_zookeeper=True,
)

node_1_2 = cluster.add_instance(
    "node_1_2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "2", "shard": "1"},
    with_zookeeper=True,
)

node_2_1 = cluster.add_instance(
    "node_2_1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "1", "shard": "2"},
    with_zookeeper=True,
)

node_2_2 = cluster.add_instance(
    "node_2_2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "2", "shard": "2"},
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
        node_1_1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster_2x2' SYNC")
        node_1_1.query("DROP TABLE IF EXISTS table_a ON CLUSTER 'cluster_2x2' SYNC")
        node_1_1.query("DROP TABLE IF EXISTS table_b ON CLUSTER 'cluster_2x2' SYNC")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def test_replicated_table():
    node_1_1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster_2x2' ("
        "x Int64"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/{shard}', '{replica}')"
        "ORDER BY x"
    )

    node_1_1.query("INSERT INTO tbl VALUES (100), (200)")
    node_2_1.query("INSERT INTO tbl VALUES (300), (400)")

    backup_name = new_backup_name()

    node_1_1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster_2x2' TO {backup_name}")

    node_1_1.query(f"DROP TABLE tbl ON CLUSTER 'cluster_2x2' SYNC")

    node_1_1.query(f"RESTORE ALL ON CLUSTER 'cluster_2x2' FROM {backup_name}")

    node_1_1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster_2x2' tbl")

    assert node_1_1.query("SELECT * FROM tbl ORDER BY x") == TSV([[100], [200]])
    assert node_1_2.query("SELECT * FROM tbl ORDER BY x") == TSV([[100], [200]])
    assert node_2_1.query("SELECT * FROM tbl ORDER BY x") == TSV([[300], [400]])
    assert node_2_2.query("SELECT * FROM tbl ORDER BY x") == TSV([[300], [400]])


def test_two_tables_with_uuid_in_zk_path():
    node_1_1.query(
        "CREATE TABLE table_a ON CLUSTER 'cluster_2x2' ("
        "x Int64"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
        "ORDER BY x"
    )

    node_1_1.query(
        "CREATE TABLE table_b ON CLUSTER 'cluster_2x2' ("
        "x Int64"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
        "ORDER BY x"
    )

    node_1_1.query("INSERT INTO table_a VALUES (100), (200)")
    node_2_1.query("INSERT INTO table_a VALUES (300), (400)")

    node_1_2.query("INSERT INTO table_b VALUES (500), (600)")
    node_2_2.query("INSERT INTO table_b VALUES (700), (800)")

    backup_name = new_backup_name()

    node_1_1.query(
        f"BACKUP TABLE table_a, TABLE table_b ON CLUSTER 'cluster_2x2' TO {backup_name}"
    )

    node_1_1.query(f"DROP TABLE table_a ON CLUSTER 'cluster_2x2' SYNC")
    node_1_1.query(f"DROP TABLE table_b ON CLUSTER 'cluster_2x2' SYNC")

    node_1_1.query(f"RESTORE ALL ON CLUSTER 'cluster_2x2' FROM {backup_name}")

    node_1_1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster_2x2' table_a")
    node_1_1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster_2x2' table_b")

    assert node_1_1.query("SELECT * FROM table_a ORDER BY x") == TSV([[100], [200]])
    assert node_1_2.query("SELECT * FROM table_a ORDER BY x") == TSV([[100], [200]])
    assert node_2_1.query("SELECT * FROM table_a ORDER BY x") == TSV([[300], [400]])
    assert node_2_2.query("SELECT * FROM table_a ORDER BY x") == TSV([[300], [400]])

    assert node_1_1.query("SELECT * FROM table_b ORDER BY x") == TSV([[500], [600]])
    assert node_1_2.query("SELECT * FROM table_b ORDER BY x") == TSV([[500], [600]])
    assert node_2_1.query("SELECT * FROM table_b ORDER BY x") == TSV([[700], [800]])
    assert node_2_2.query("SELECT * FROM table_b ORDER BY x") == TSV([[700], [800]])
