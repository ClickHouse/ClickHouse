import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import list_s3_objects

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups.xml"],
    stay_alive=True,
    with_minio=True,
)

# `test_database_backup_metadata_with_quoted_locator_loads_on_restart` rewrites a database metadata file
# in place, and such a file only exists when the metadata lives on the local disk, so that test runs on an
# instance which keeps the local database disk.
instance_local_metadata = cluster.add_instance(
    "instance_local_metadata",
    main_configs=["configs/backups.xml"],
    stay_alive=True,
    with_minio=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def cleanup_backup_files(instance):
    instance.exec_in_container(["bash", "-c", "rm -rf /backups/"])
    instance.exec_in_container(["bash", "-c", "rm -rf /local_plain/"])

    minio = cluster.minio_client
    s3_objects = list_s3_objects(minio, cluster.minio_bucket, prefix="")
    for s3_object in s3_objects:
        minio.remove_object(cluster.minio_bucket, s3_object)


@pytest.mark.parametrize(
    "backup_destination",
    [
        "File('test_database_backup_file')",
        "Disk('backup_disk_local', 'test_database_backup')",
        "Disk('backup_disk_s3_plain', 'test_database_backup')",
        "Disk('backup_disk_object_storage_local_plain', 'test_database_backup')",
    ],
)
def test_database_backup_database(backup_destination):
    cleanup_backup_files(instance)

    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_database;
        DROP DATABASE IF EXISTS test_database_backup;

        CREATE DATABASE test_database;

        CREATE TABLE test_database.test_table_1 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table_1 VALUES (0, 'test_database.test_table_1');

        CREATE TABLE test_database.test_table_2 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table_2 VALUES (0, 'test_database.test_table_2');

        CREATE TABLE test_database.test_table_3 (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table_3 VALUES (0, 'test_database.test_table_3');

        BACKUP DATABASE test_database TO {backup_destination};
        CREATE DATABASE test_database_backup ENGINE = Backup('test_database', {backup_destination});
    """
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table_1")
        == "0\ttest_database.test_table_1\n"
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table_2")
        == "0\ttest_database.test_table_2\n"
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table_3")
        == "0\ttest_database.test_table_3\n"
    )

    instance.restart_clickhouse()

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table_1")
        == "0\ttest_database.test_table_1\n"
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table_2")
        == "0\ttest_database.test_table_2\n"
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table_3")
        == "0\ttest_database.test_table_3\n"
    )

    instance.query("DROP DATABASE test_database_backup")
    instance.query("DROP DATABASE test_database")
    cleanup_backup_files(instance)


@pytest.mark.parametrize(
    "backup_destination",
    [
        "File('test_table_backup_file')",
        "Disk('backup_disk_local', 'test_table_backup')",
        "Disk('backup_disk_s3_plain', 'test_table_backup')",
        "Disk('backup_disk_object_storage_local_plain', 'test_table_backup')",
    ],
)
def test_database_backup_table(backup_destination):
    cleanup_backup_files(instance)

    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_database;
        DROP DATABASE IF EXISTS test_table_backup;

        CREATE DATABASE test_database;

        CREATE TABLE test_database.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table VALUES (0, 'test_database.test_table');

        BACKUP TABLE test_database.test_table TO {backup_destination};
        CREATE DATABASE test_table_backup ENGINE = Backup('test_database', {backup_destination});
    """
    )

    assert (
        instance.query("SELECT id, value FROM test_table_backup.test_table")
        == "0\ttest_database.test_table\n"
    )

    instance.restart_clickhouse()

    assert (
        instance.query("SELECT id, value FROM test_table_backup.test_table")
        == "0\ttest_database.test_table\n"
    )

    instance.query("DROP DATABASE test_table_backup")
    instance.query("DROP DATABASE test_database")
    cleanup_backup_files(instance)


@pytest.mark.parametrize(
    "backup_destination",
    [
        "Disk('backup_disk_s3_plain', 'test_database_backup')",
    ],
)
def test_multiple_databases_from_same_backup(backup_destination):
    # Written by @orloffv in https://github.com/ClickHouse/ClickHouse/pull/83220
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/83219
    cleanup_backup_files(instance)

    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_database SYNC;
        DROP DATABASE IF EXISTS test_database_backup_1 SYNC;
        DROP DATABASE IF EXISTS test_database_backup_2 SYNC;

        CREATE DATABASE test_database;

        CREATE TABLE test_database.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table VALUES (1, 'from_backup');

        BACKUP DATABASE test_database TO {backup_destination};

        CREATE DATABASE test_database_backup_1 ENGINE=Backup('test_database', {backup_destination});
        CREATE DATABASE test_database_backup_2 ENGINE=Backup('test_database', {backup_destination});
    """
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup_1.test_table")
        == "1\tfrom_backup\n"
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup_2.test_table")
        == "1\tfrom_backup\n"
    )

    # Both databases must still read after a restart: the storage policy name is derived
    # again on every open, so it has to come out identical.
    instance.restart_clickhouse()

    assert (
        instance.query("SELECT id, value FROM test_database_backup_1.test_table")
        == "1\tfrom_backup\n"
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup_2.test_table")
        == "1\tfrom_backup\n"
    )

    instance.query("DROP DATABASE IF EXISTS test_database_backup_1 SYNC")
    instance.query("DROP DATABASE IF EXISTS test_database_backup_2 SYNC")
    instance.query("DROP DATABASE IF EXISTS test_database SYNC")
    cleanup_backup_files(instance)


@pytest.mark.parametrize(
    "backup_destination",
    [
        "File('test_database_backup_file')",
        "Disk('backup_disk_local', 'test_database_backup')",
        "Disk('backup_disk_s3_plain', 'test_database_backup')",
        "Disk('backup_disk_object_storage_local_plain', 'test_database_backup')",
    ],
)
def test_database_backup_unavailable_but_server_starts(backup_destination):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/83187
    # When a Backup database refers to a backup that became unavailable (e.g. the backup
    # files were deleted or the underlying storage is inaccessible), the server must still
    # start. The Backup database is loaded without any tables.
    cleanup_backup_files(instance)

    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_database SYNC;
        DROP DATABASE IF EXISTS test_database_backup SYNC;

        CREATE DATABASE test_database;

        CREATE TABLE test_database.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table VALUES (0, 'test_database.test_table');

        BACKUP DATABASE test_database TO {backup_destination};
        CREATE DATABASE test_database_backup ENGINE = Backup('test_database', {backup_destination});
    """
    )

    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table")
        == "0\ttest_database.test_table\n"
    )

    # Make the backup unavailable and restart the server.
    cleanup_backup_files(instance)
    instance.restart_clickhouse()

    # The server must start despite the unavailable backup.
    assert instance.query("SELECT 1") == "1\n"

    # The Backup database is still attached, but loaded without any tables.
    assert (
        instance.query(
            "SELECT name FROM system.databases WHERE name = 'test_database_backup'"
        )
        == "test_database_backup\n"
    )
    assert (
        instance.query(
            "SELECT count() FROM system.tables WHERE database = 'test_database_backup'"
        )
        == "0\n"
    )

    # The original (non-backup) database is unaffected by the unavailable backup.
    assert (
        instance.query("SELECT id, value FROM test_database.test_table")
        == "0\ttest_database.test_table\n"
    )

    instance.query("DROP DATABASE IF EXISTS test_database_backup SYNC")
    instance.query("DROP DATABASE IF EXISTS test_database SYNC")
    cleanup_backup_files(instance)


def test_database_backup_metadata_with_quoted_locator_loads_on_restart():
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/118349
    # An older server regenerated the definition of a `Backup` database with the locator quoted into a
    # string literal, and `ALTER DATABASE ... MODIFY COMMENT` wrote that back into `metadata/<db>.sql`.
    # The next start replays the stored full `ATTACH DATABASE ... ENGINE = Backup(...)` statement, which
    # is neither the short `ATTACH` nor a force-restore load, so it has to accept that form on its own.
    #
    # The metadata file is rewritten in place here, so this runs on the instance whose metadata is a file
    # on the local disk rather than an object on a remote database disk.
    instance = instance_local_metadata

    cleanup_backup_files(instance)

    instance.query(
        """
        DROP DATABASE IF EXISTS test_database SYNC;
        DROP DATABASE IF EXISTS test_database_backup SYNC;

        CREATE DATABASE test_database;

        CREATE TABLE test_database.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_database.test_table VALUES (0, 'test_database.test_table');

        BACKUP DATABASE test_database TO File('test_database_backup_file');
        CREATE DATABASE test_database_backup ENGINE = Backup('test_database', File('test_database_backup_file'));
    """
    )
    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table")
        == "0\ttest_database.test_table\n"
    )

    # The metadata file exactly as a pre-fix server left it after a comment change.
    instance.stop_clickhouse()
    metadata = (
        "ATTACH DATABASE test_database_backup\n"
        "ENGINE = Backup('test_database', 'File(\\'test_database_backup_file\\')')\n"
        "COMMENT 'written by an older server'\n"
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "cat > /var/lib/clickhouse/metadata/test_database_backup.sql <<'SQL'\n"
            + metadata
            + "SQL\n",
        ],
        user="root",
    )
    assert "'File(\\'test_database_backup_file\\')'" in instance.exec_in_container(
        ["cat", "/var/lib/clickhouse/metadata/test_database_backup.sql"]
    )
    instance.start_clickhouse()

    # The server started and the database loaded with its tables and comment.
    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table")
        == "0\ttest_database.test_table\n"
    )
    assert (
        instance.query(
            "SELECT comment FROM system.databases WHERE name = 'test_database_backup'"
        )
        == "written by an older server\n"
    )
    # The definition is regenerated with the locator as the function it is.
    assert (
        "ENGINE = Backup('test_database', File('test_database_backup_file'))"
        in instance.query("SHOW CREATE DATABASE test_database_backup FORMAT TSVRaw")
    )

    # A comment change on this server writes the function form, and that survives a restart too.
    instance.query(
        "ALTER DATABASE test_database_backup MODIFY COMMENT 'written by this server'"
    )
    instance.restart_clickhouse()
    assert (
        instance.query("SELECT id, value FROM test_database_backup.test_table")
        == "0\ttest_database.test_table\n"
    )
    assert (
        instance.query(
            "SELECT comment FROM system.databases WHERE name = 'test_database_backup'"
        )
        == "written by this server\n"
    )

    instance.query("DROP DATABASE test_database_backup SYNC")
    instance.query("DROP DATABASE test_database SYNC")
