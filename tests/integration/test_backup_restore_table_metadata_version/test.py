import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/backup_disk.xml"],
    external_dirs=["/backups/"],
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


def test_restore_table_metadata_version_two_replicas(start_cluster):
    zk_path = "/clickhouse/tables/test_db/two_r_test"

    replica1.query("CREATE DATABASE IF NOT EXISTS test_db")
    replica1.query(
        f"""
        CREATE TABLE test_db.two_r_test (id UInt64, name Nullable(String))
        ENGINE = ReplicatedReplacingMergeTree('{zk_path}', '{{replica}}')
        ORDER BY id
        """
    )
    replica1.query(
        "INSERT INTO test_db.two_r_test SELECT number, toString(number) FROM numbers(10)"
    )
    replica1.query(
        "ALTER TABLE test_db.two_r_test ADD COLUMN surname Nullable(String) AFTER name"
    )
    replica1.query(
        "INSERT INTO test_db.two_r_test SELECT number, toString(number), toString(number) FROM numbers(10, 10)"
    )
    assert (
        replica1.query(
            "SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = 'two_r_test'"
        ).strip()
        == "1"
    )

    backup_name = new_backup_name()
    replica1.query(f"BACKUP TABLE test_db.two_r_test TO {backup_name}")

    replica1.query("DROP TABLE test_db.two_r_test SYNC")

    # replica2 joins the same ZK path fresh (stat = 0, in-memory metadata_version = 0).
    replica2.query("CREATE DATABASE IF NOT EXISTS test_db")
    replica2.query(
        f"""
        CREATE TABLE test_db.two_r_test (id UInt64, name Nullable(String), surname Nullable(String))
        ENGINE = ReplicatedReplacingMergeTree('{zk_path}', '{{replica}}')
        ORDER BY id
        """
    )
    assert (
        replica2.query(
            "SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = 'two_r_test'"
        ).strip()
        == "0"
    )

    replica1.query(f"RESTORE TABLE test_db.two_r_test FROM {backup_name}")

    assert (
        replica1.query(
            "SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = 'two_r_test'"
        ).strip()
        == "1"
    )

    assert_eq_with_retry(
        replica2,
        "SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = 'two_r_test'",
        "1",
    )

    replica2.query("SYSTEM SYNC REPLICA test_db.two_r_test", timeout=60)
    replica2.query("OPTIMIZE TABLE test_db.two_r_test FINAL", timeout=120)
    assert (
        replica2.query(
            "SELECT count() FROM system.parts WHERE database = 'test_db' AND table = 'two_r_test' AND active"
        ).strip()
        == "1"
    )

    replica1.query("DROP TABLE test_db.two_r_test SYNC")
    replica2.query("DROP TABLE test_db.two_r_test SYNC")


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


# `restore_table_data=0` skips the data without setting `structure_only`, so the metadata
# version must still be applied: `structure_only` alone is not the governing condition.
def test_restore_table_metadata_version_without_table_data(start_cluster):
    create_table("t3")
    node.query(
        "ALTER TABLE test_db.t3 ADD COLUMN surname Nullable(String) SETTINGS alter_sync = 2"
    )
    assert metadata_version("t3") == "1"

    backup_name = new_backup_name()
    node.query(f"BACKUP TABLE test_db.t3 TO {backup_name}")
    node.query("DROP TABLE test_db.t3 SYNC")
    node.query(
        f"RESTORE TABLE test_db.t3 FROM {backup_name} SETTINGS restore_table_data=0"
    )

    assert metadata_version("t3") == "1"

    node.query("DROP TABLE test_db.t3 SYNC")


# Restoring an older backup into a table which has been altered since must not move the
# table's metadata version backwards.
def test_restore_table_metadata_version_never_decreases(start_cluster):
    create_table("t4")
    node.query(
        "ALTER TABLE test_db.t4 ADD COLUMN surname Nullable(String) SETTINGS alter_sync = 2"
    )

    backup_name = new_backup_name()
    node.query(f"BACKUP TABLE test_db.t4 TO {backup_name}")

    node.query("ALTER TABLE test_db.t4 ADD COLUMN extra UInt8 SETTINGS alter_sync = 2")
    assert metadata_version("t4") == "2"

    node.query(
        f"RESTORE TABLE test_db.t4 FROM {backup_name} "
        "SETTINGS allow_different_table_def=1"
    )
    assert metadata_version("t4") == "2"

    node.query("DROP TABLE test_db.t4 SYNC")


# Both replicas of a `Replicated` database must apply the ALTER_METADATA entry created by
# RESTORE, which goes through the ZooKeeperMetadataTransaction branch of executeMetadataAlter.
def test_replicated_database_second_replica_applies_restored_version(start_cluster):
    version_query = (
        "SELECT metadata_version FROM system.tables "
        "WHERE database = 'repl_db2' AND name = 't'"
    )
    create_db_query = (
        "CREATE DATABASE repl_db2 "
        "ENGINE = Replicated('/clickhouse/databases/repl_db2', '{shard}', '{replica}')"
    )
    replica1.query(create_db_query)
    replica2.query(create_db_query)
    try:
        replica1.query(
            "CREATE TABLE repl_db2.t (id UInt64, name Nullable(String)) "
            "ENGINE = ReplicatedReplacingMergeTree ORDER BY id"
        )
        replica1.query("ALTER TABLE repl_db2.t ADD COLUMN surname Nullable(String)")
        assert replica1.query(version_query).strip() == "1"

        backup_name = new_backup_name()
        replica1.query(
            f"BACKUP DATABASE repl_db2 ON CLUSTER 'cluster' TO {backup_name}"
        )
        replica1.query("DROP DATABASE repl_db2 SYNC")
        replica2.query("DROP DATABASE repl_db2 SYNC")
        replica1.query(
            f"RESTORE DATABASE repl_db2 ON CLUSTER 'cluster' FROM {backup_name}"
        )

        for replica in (replica1, replica2):
            assert_eq_with_retry(replica, version_query, "1")

        # The replication queues are not stuck on that entry: a later ALTER still goes through.
        replica2.query(
            "ALTER TABLE repl_db2.t ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
        )
        for replica in (replica1, replica2):
            assert_eq_with_retry(replica, version_query, "2")
    finally:
        replica1.query("DROP DATABASE IF EXISTS repl_db2 SYNC")
        replica2.query("DROP DATABASE IF EXISTS repl_db2 SYNC")
