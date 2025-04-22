import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/cluster.xml", "configs/backup_disk.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/cluster.xml", "configs/backup_disk.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 2},
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
    node1.query("CREATE DATABASE IF NOT EXISTS test_db;")
    node2.query("CREATE DATABASE IF NOT EXISTS test_db;")
    node1.query("DROP TABLE IF EXISTS test_db.t")
    node2.query("DROP TABLE IF EXISTS test_db.t")

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
    node1.query(create_table_query)
    node2.query(create_table_query)


    node1.query("INSERT INTO test_db.t SELECT number, toString(number) FROM numbers(30);")
    # node.query("SYSTEM STOP REPLICATION QUEUES backup_test_db.t;")
    node2.query("SYSTEM SYNC REPLICA test_db.t;")
    assert node2.query("SELECT count() FROM test_db.t;") == "30\n"

    # Modify table (add column) so that metadata version is incremented
    node1.query("ALTER TABLE test_db.t ADD COLUMN `surname` Nullable(String) after `name`")

    node1.query("INSERT INTO test_db.t (`id`, `name`, `surname`) VALUES (3, 'Test4', 'Test4')")
    node1.query("INSERT INTO test_db.t (`id`, `name`, `surname`) VALUES (4, 'Test5', 'Test5')")

    node2.query("SYSTEM SYNC REPLICA test_db.t;")
    assert node2.query("SELECT count() FROM test_db.t;") == "32\n"

    assert node2.query("select metadata_version from system.tables where database='test_db' and name='t';") == "1\n"

    # backup, drop, restore
    node1.query("BACKUP TABLE test_db.t TO Disk('backups', 'n1/bkp1');")
    node2.query("BACKUP TABLE test_db.t TO Disk('backups', 'n2/bkp1');")

    node1.query("DROP TABLE test_db.t SYNC;")
    node2.query("DROP TABLE test_db.t SYNC;")

    node1.query("RESTORE TABLE test_db.t FROM Disk('backups', 'n1/bkp1')")
    node2.query("RESTORE TABLE test_db.t FROM Disk('backups', 'n2/bkp1') SETTINGS structure_only=true")


    assert node2.query("select metadata_version from system.tables where database='test_db' and name='t';") == "1\n"

    node2.query("SYSTEM SYNC REPLICA test_db.t;")

