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

# based on https://github.com/ClickHouse/ClickHouse/issues/67457

def test_restore_table_metadata_version(start_cluster):
    node.query("CREATE DATABASE IF NOT EXISTS test_db;")
    node.query("DROP TABLE IF EXISTS test_db.t")

    create_table_query = """
        CREATE TABLE test_db.t (
            id UInt64,
            name Nullable(String)
        ) ENGINE = ReplicatedReplacingMergeTree(
              '/clickhouse/tables/test_db/t', '{replica}'
        ) 
        PRIMARY KEY id
        ORDER BY id;
    """
    node.query(create_table_query)


    node.query("INSERT INTO test_db.t SELECT number, toString(number) FROM numbers(30);")

    # Modify table (add column) so that metadata version is incremented
    node.query("ALTER TABLE test_db.t ADD COLUMN `surname` Nullable(String) after `name` SETTINGS alter_sync = 2")

    node.query("SYSTEM SYNC REPLICA test_db.t")

    # backup, drop, restore
    node.query("BACKUP TABLE test_db.t TO Disk('backups', 'bkp1') SETTINGS structure_only=true")

    node.query("DROP TABLE test_db.t SYNC;")

    node.query("RESTORE TABLE test_db.t FROM Disk('backups', 'bkp1') SETTINGS structure_only=true")


    assert node.query("select metadata_version from system.tables where database='test_db' and name='t';") == "1\n"
