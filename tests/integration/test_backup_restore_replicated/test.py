import pytest
import os.path
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    macros={"replica": "node1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    macros={"replica": "node2"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' NO DELAY")
    finally:
        cluster.shutdown()


def create_table(instance = None):
    on_cluster_clause = "" if instance else "ON CLUSTER 'cluster'"
    instance_to_execute = instance if instance else node1
    instance_to_execute.query(
        "CREATE TABLE tbl " + on_cluster_clause + " ("
            "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )


def drop_table(instance = None):
    on_cluster_clause = "" if instance else "ON CLUSTER 'cluster'"
    instance_to_execute = instance if instance else node1
    instance_to_execute.query(f"DROP TABLE tbl {on_cluster_clause} NO DELAY")


def insert_data(instance = None):
    instance1_to_execute = instance if instance else node1
    instance2_to_execute = instance if instance else node2
    instance1_to_execute.query("INSERT INTO tbl VALUES (1, 'Don''t')")
    instance2_to_execute.query("INSERT INTO tbl VALUES (2, 'count')")
    instance1_to_execute.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (3, 'your')")
    instance2_to_execute.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (4, 'chickens')")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}.zip')"


def get_path_to_backup(instance, backup_name):
    return os.path.join(
        instance.path,
        "backups",
        backup_name.removeprefix("Disk('backups', '").removesuffix("')"),
    )


def test_backup_and_restore():
    create_table()
    insert_data()

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(f"BACKUP TABLE tbl TO {backup_name}")

    # Drop table on both nodes.
    drop_table()

    # Restore from backup on node2.
    os.link(
        get_path_to_backup(node1, backup_name), get_path_to_backup(node2, backup_name)
    )
    node2.query(f"RESTORE TABLE tbl FROM {backup_name}")

    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )

    # Data should be replicated to node1.
    create_table(node1)
    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )
