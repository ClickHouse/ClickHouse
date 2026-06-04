import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/backup_disk.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
    stay_alive=True,
)

replica1 = cluster.add_instance(
    "replica1",
    main_configs=["configs/backup_disk.xml", "configs/cluster.xml"],
    external_dirs=["/backups/"],
    with_zookeeper=True,
    macros={"shard": "shard1", "replica": "r1"},
)

replica2 = cluster.add_instance(
    "replica2",
    main_configs=["configs/backup_disk.xml", "configs/cluster.xml"],
    external_dirs=["/backups/"],
    with_zookeeper=True,
    macros={"shard": "shard1", "replica": "r2"},
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


def metadata_version(table):
    return node.query(
        f"SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = '{table}'"
    ).strip()


def create_table(table):
    node.query("CREATE DATABASE IF NOT EXISTS test_db")
    node.query(
        f"""
        CREATE TABLE test_db.{table} (id UInt64, name Nullable(String))
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/test_db/{table}', '{{replica}}')
        ORDER BY id
        """
    )


# Reproduces https://github.com/ClickHouse/ClickHouse/issues/67457
def test_restore_table_metadata_version(start_cluster):
    create_table("t1")
    node.query(
        "INSERT INTO test_db.t1 SELECT number, toString(number) FROM numbers(10)"
    )
    node.query(
        "ALTER TABLE test_db.t1 ADD COLUMN surname Nullable(String) AFTER name SETTINGS alter_sync = 2"
    )
    node.query(
        "INSERT INTO test_db.t1 SELECT number, toString(number), toString(number) FROM numbers(10, 10)"
    )
    assert metadata_version("t1") == "1"

    backup_name = new_backup_name()
    node.query(f"BACKUP TABLE test_db.t1 TO {backup_name}")
    node.query("DROP TABLE test_db.t1 SYNC")
    node.query(f"RESTORE TABLE test_db.t1 FROM {backup_name}")

    assert metadata_version("t1") == "1"

    # Merges of parts with metadata version 1 are not blocked (the symptom of #67457).
    node.query("OPTIMIZE TABLE test_db.t1 FINAL", timeout=120)
    assert (
        node.query(
            "SELECT count() FROM system.parts WHERE database = 'test_db' AND table = 't1' AND active"
        ).strip()
        == "1"
    )

    # ALTER works after the restore (the version in ZooKeeper matches the in-memory one).
    node.query(
        "ALTER TABLE test_db.t1 ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
    )
    assert metadata_version("t1") == "2"

    # The restored metadata version survives a server restart.
    node.restart_clickhouse()
    assert_eq_with_retry(
        node,
        "SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = 't1'",
        "2",
    )

    node.query("DROP TABLE test_db.t1 SYNC")


# Reproduces the case when a table of a `Replicated` database is known to the database
# but has not been created on one of its replicas yet at the moment of a backup.
# Such a replica writes the table definition to its part of the backup without having
# a local storage for the table, so the metadata version must be taken from ZooKeeper.
def test_replicated_database_table_not_created_on_replica(start_cluster):
    replica1.query(
        "CREATE DATABASE repl_db ENGINE = Replicated('/clickhouse/databases/repl_db', '{shard}', '{replica}')"
    )
    replica2.query(
        "CREATE DATABASE repl_db ENGINE = Replicated('/clickhouse/databases/repl_db', '{shard}', '{replica}')"
    )
    try:
        # Pause the database replication queue on replica2: the table will be known to the database
        # (the backup reads table definitions from ZooKeeper) but not created on replica2.
        replica2.query(
            "SYSTEM ENABLE FAILPOINT database_replicated_stop_entry_execution"
        )

        replica1.query(
            """
            CREATE TABLE repl_db.t (id UInt64, name Nullable(String))
            ENGINE = ReplicatedReplacingMergeTree ORDER BY id
            """,
            settings={"distributed_ddl_task_timeout": 0},
        )
        replica1.query(
            "INSERT INTO repl_db.t SELECT number, toString(number) FROM numbers(10)"
        )
        replica1.query(
            "ALTER TABLE repl_db.t ADD COLUMN surname Nullable(String) AFTER name",
            settings={"distributed_ddl_task_timeout": 0, "alter_sync": 1},
        )
        replica1.query(
            "INSERT INTO repl_db.t SELECT number, toString(number), toString(number) FROM numbers(10, 10)"
        )
        assert (
            replica1.query(
                "SELECT metadata_version FROM system.tables WHERE database = 'repl_db' AND name = 't'"
            ).strip()
            == "1"
        )
        # The table has not been created on replica2.
        assert (
            replica2.query(
                "SELECT count() FROM system.tables WHERE database = 'repl_db' AND name = 't'"
            ).strip()
            == "0"
        )

        backup_name = new_backup_name()
        replica1.query(f"BACKUP DATABASE repl_db ON CLUSTER 'cluster' TO {backup_name}")

        replica2.query(
            "SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution"
        )
        replica2.query("SYSTEM SYNC DATABASE REPLICA repl_db")
        replica1.query("DROP DATABASE repl_db SYNC")
        replica2.query("DROP DATABASE repl_db SYNC")

        # Restore the part of the backup written by replica2 (which had no local storage for the table).
        replica1.query(
            f"RESTORE DATABASE repl_db FROM {backup_name} SETTINGS replica_num_in_backup = 2"
        )

        assert (
            replica1.query(
                "SELECT metadata_version FROM system.tables WHERE database = 'repl_db' AND name = 't'"
            ).strip()
            == "1"
        )
        assert replica1.query("SELECT count() FROM repl_db.t").strip() == "20"

        # Merges of parts with metadata version 1 are not blocked.
        replica1.query("OPTIMIZE TABLE repl_db.t FINAL", timeout=120)
        assert (
            replica1.query(
                "SELECT count() FROM system.parts WHERE database = 'repl_db' AND table = 't' AND active"
            ).strip()
            == "1"
        )

        # ALTER works after the restore (the version in ZooKeeper matches the in-memory one).
        replica1.query(
            "ALTER TABLE repl_db.t ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
        )
        assert (
            replica1.query(
                "SELECT metadata_version FROM system.tables WHERE database = 'repl_db' AND name = 't'"
            ).strip()
            == "2"
        )
    finally:
        replica2.query(
            "SYSTEM DISABLE FAILPOINT database_replicated_stop_entry_execution"
        )
        replica1.query("DROP DATABASE IF EXISTS repl_db SYNC")
        replica2.query("DROP DATABASE IF EXISTS repl_db SYNC")


def test_restore_table_metadata_version_structure_only(start_cluster):
    create_table("t2")
    node.query(
        "ALTER TABLE test_db.t2 ADD COLUMN surname Nullable(String) AFTER name SETTINGS alter_sync = 2"
    )
    assert metadata_version("t2") == "1"

    backup_name = new_backup_name()
    node.query(
        f"BACKUP TABLE test_db.t2 TO {backup_name} SETTINGS structure_only = true"
    )
    node.query("DROP TABLE test_db.t2 SYNC")
    node.query(
        f"RESTORE TABLE test_db.t2 FROM {backup_name} SETTINGS structure_only = true"
    )

    assert metadata_version("t2") == "1"

    node.query(
        "ALTER TABLE test_db.t2 ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
    )
    assert metadata_version("t2") == "2"

    node.query("DROP TABLE test_db.t2 SYNC")
