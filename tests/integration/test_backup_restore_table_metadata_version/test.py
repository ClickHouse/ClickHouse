import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/cluster.xml", "configs/backup_disk.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


backup_id_counter = 0

def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"

# based on https://github.com/ClickHouse/ClickHouse/issues/67457

def test_restore_table_metadata_version(start_cluster):
    node.query("CREATE DATABASE IF NOT EXISTS test_db;")
    node.query("DROP TABLE IF EXISTS test_db.t1 SYNC")
    node.query("DROP TABLE IF EXISTS test_db.t2 SYNC")

    for table_name in ["t1", "t2"]:
        create_table_query = f"""
            CREATE TABLE test_db.{table_name} (
                id UInt64,
                name Nullable(String)
            ) ENGINE = ReplicatedReplacingMergeTree(
                  '/clickhouse/tables/test_db/{table_name}', '{{replica}}'
            ) 
            PRIMARY KEY id
            ORDER BY id;
        """
        node.query(create_table_query)

    node.query("INSERT INTO test_db.t1 SELECT number, toString(number) FROM numbers(30);")

    # Modify table (add column) so that metadata version is incremented
    node.query("ALTER TABLE test_db.t1 ADD COLUMN `surname` Nullable(String) after `name` SETTINGS alter_sync = 2")

    backup_name = new_backup_name()
    node.query(f"BACKUP TABLE test_db.t1 TO {backup_name} SETTINGS structure_only=true")
    node.query("DROP TABLE test_db.t1 SYNC;")
    node.query(f"RESTORE TABLE test_db.t1 AS test_db.t2 FROM {backup_name} SETTINGS structure_only=true, allow_different_table_def=true")


    assert node.query("select metadata_version from system.tables where database='test_db' and name='t2';") == "1\n"
    node.query("DROP TABLE IF EXISTS test_db.t1 SYNC")
    node.query("DROP TABLE IF EXISTS test_db.t2 SYNC")
