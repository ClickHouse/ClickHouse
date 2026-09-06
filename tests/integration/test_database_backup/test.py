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


def read_database_def(path):
    return instance.exec_in_container(["cat", path])


def write_database_def(path, text):
    # A quoted heredoc delimiter keeps the shell from touching the backslashes of the SQL literal.
    instance.exec_in_container(
        ["bash", "-c", f"cat > {path} <<'DATABASE_DEF_EOF'\n{text}\nDATABASE_DEF_EOF"]
    )


def quoted(locator):
    # The spelling an older server persisted: the text of the locator as one string literal.
    return "'" + locator.replace("\\", "\\\\").replace("'", "\\'") + "'"


def test_database_backup_legacy_quoted_locator():
    # An older server wrote a Backup database's locator as a single string literal instead of the
    # nested function, so a backup taken then carries that spelling. Reading such a definition must
    # parse the literal back into the function form: the engine only accepts a function, and the
    # definition comparison of RESTORE compares the two spellings textually.
    cleanup_backup_files(instance)

    inner = "Disk('backup_disk_local', 'legacy_inner')"
    outer = "Disk('backup_disk_local', 'legacy_outer')"

    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_legacy_source SYNC;
        DROP DATABASE IF EXISTS test_legacy_view SYNC;

        CREATE DATABASE test_legacy_source;
        CREATE TABLE test_legacy_source.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_legacy_source.test_table VALUES (0, 'test_legacy_source.test_table');

        BACKUP DATABASE test_legacy_source TO {inner};
        CREATE DATABASE test_legacy_view ENGINE = Backup('test_legacy_source', {inner});
        BACKUP DATABASE test_legacy_view TO {outer};
    """
    )

    # A Backup database contributes no tables to a backup, so the archive holds just its definition.
    archived_def_path = "/backups/legacy_outer/metadata/test_legacy_view.sql"
    archived_def = read_database_def(archived_def_path)
    assert inner in archived_def, archived_def

    legacy_def = archived_def.replace(inner, quoted(inner))
    write_database_def(archived_def_path, legacy_def)
    assert read_database_def(archived_def_path).strip() == legacy_def.strip()

    # Arm 1: the target does not exist, so RESTORE creates it by executing the archived definition.
    instance.query("DROP DATABASE test_legacy_view SYNC")
    instance.query(f"RESTORE DATABASE test_legacy_view FROM {outer}")
    # TSVRaw so the locator is compared as written rather than through TSV escaping.
    assert (
        instance.query(
            "SELECT engine_full FROM system.databases WHERE name = 'test_legacy_view' FORMAT TSVRaw"
        )
        == f"Backup('test_legacy_source', {inner})\n"
    )
    assert (
        instance.query("SELECT id, value FROM test_legacy_view.test_table")
        == "0\ttest_legacy_source.test_table\n"
    )

    # Arm 2: the target exists in the current spelling, so RESTORE compares the two definitions.
    instance.query(f"RESTORE DATABASE test_legacy_view FROM {outer}")
    assert (
        instance.query("SELECT id, value FROM test_legacy_view.test_table")
        == "0\ttest_legacy_source.test_table\n"
    )

    # Control: two locators that differ in more than spelling must still compare unequal.
    other = quoted("Disk('backup_disk_local', 'legacy_other')")
    write_database_def(archived_def_path, archived_def.replace(inner, other))
    assert "CANNOT_RESTORE_DATABASE" in instance.query_and_get_error(
        f"RESTORE DATABASE test_legacy_view FROM {outer}"
    )

    # Control: a literal that does not decode must fail, and the error must not echo it - the locator
    # of an S3 destination carries a secret access key.
    write_database_def(
        archived_def_path, archived_def.replace(inner, "'not a locator SEKRIT_LOCATOR'")
    )
    error = instance.query_and_get_error(
        f"RESTORE DATABASE test_legacy_view FROM {outer}"
    )
    assert "BAD_ARGUMENTS" in error, error
    assert "SEKRIT_LOCATOR" not in error, error

    instance.query("DROP DATABASE IF EXISTS test_legacy_view SYNC")
    instance.query("DROP DATABASE IF EXISTS test_legacy_source SYNC")
    cleanup_backup_files(instance)


def test_database_backup_comment_survives_restart():
    # ALTER DATABASE ... MODIFY COMMENT rewrites the on-disk metadata from the database's own create
    # query, so whatever that query prints is what the server has to load next time it starts. A
    # locator printed as a string literal is not accepted on load, and because metadata loading
    # aborts on the first failure, one such file stopped the server from starting at all.
    cleanup_backup_files(instance)

    destination = "Disk('backup_disk_local', 'test_comment_backup')"
    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_comment_source SYNC;
        DROP DATABASE IF EXISTS test_comment_view SYNC;

        CREATE DATABASE test_comment_source;
        CREATE TABLE test_comment_source.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_comment_source.test_table VALUES (0, 'test_comment_source.test_table');

        BACKUP DATABASE test_comment_source TO {destination};
        CREATE DATABASE test_comment_view ENGINE = Backup('test_comment_source', {destination});
        ALTER DATABASE test_comment_view MODIFY COMMENT 'a comment';
    """
    )

    instance.restart_clickhouse()

    # The server came back, and the rewritten metadata loaded: the comment is there, the locator is
    # still the nested function, and the data still reads through the reattached database.
    assert instance.query("SELECT 1") == "1\n"
    assert (
        instance.query(
            "SELECT comment FROM system.databases WHERE name = 'test_comment_view' FORMAT TSVRaw"
        )
        == "a comment\n"
    )
    assert (
        instance.query(
            "SELECT engine_full FROM system.databases WHERE name = 'test_comment_view' FORMAT TSVRaw"
        )
        == f"Backup('test_comment_source', {destination})\n"
    )
    assert (
        instance.query("SELECT id, value FROM test_comment_view.test_table")
        == "0\ttest_comment_source.test_table\n"
    )

    instance.query("DROP DATABASE IF EXISTS test_comment_view SYNC")
    instance.query("DROP DATABASE IF EXISTS test_comment_source SYNC")
    cleanup_backup_files(instance)


def test_database_backup_legacy_quoted_locator_in_metadata():
    # A server that persisted the locator as a string literal leaves that spelling behind in
    # metadata/<db>.sql, and metadata loading rethrows the first failure out of startup: such a file
    # keeps the server down, so no DDL can be issued to repair or drop the database it defines.
    cleanup_backup_files(instance)

    destination = "Disk('backup_disk_local', 'legacy_metadata')"
    instance.query(
        f"""
        DROP DATABASE IF EXISTS test_legacy_metadata_source SYNC;
        DROP DATABASE IF EXISTS test_legacy_metadata_view SYNC;

        CREATE DATABASE test_legacy_metadata_source;
        CREATE TABLE test_legacy_metadata_source.test_table (id UInt64, value String) ENGINE=MergeTree ORDER BY id;
        INSERT INTO test_legacy_metadata_source.test_table VALUES (0, 'test_legacy_metadata_source.test_table');

        BACKUP DATABASE test_legacy_metadata_source TO {destination};
        CREATE DATABASE test_legacy_metadata_view ENGINE = Backup('test_legacy_metadata_source', {destination});
        ALTER DATABASE test_legacy_metadata_view MODIFY COMMENT 'a comment';
    """
    )

    def_path = "/var/lib/clickhouse/metadata/test_legacy_metadata_view.sql"
    definition = read_database_def(def_path)
    assert destination in definition, definition
    write_database_def(def_path, definition.replace(destination, quoted(destination)))

    instance.restart_clickhouse()

    # The server came back with the database attached: the string form was parsed into the function
    # the engine opens, and reading through it still reaches the backup.
    assert instance.query("SELECT 1") == "1\n"
    # TSVRaw so the locator is compared as written rather than through TSV escaping.
    assert (
        instance.query(
            "SELECT engine_full FROM system.databases WHERE name = 'test_legacy_metadata_view' FORMAT TSVRaw"
        )
        == f"Backup('test_legacy_metadata_source', {destination})\n"
    )
    assert (
        instance.query("SELECT id, value FROM test_legacy_metadata_view.test_table")
        == "0\ttest_legacy_metadata_source.test_table\n"
    )

    # The next rewrite of that file persists the function form, so the spelling does not come back.
    instance.query("ALTER DATABASE test_legacy_metadata_view MODIFY COMMENT 'another comment'")
    assert destination in read_database_def(def_path), read_database_def(def_path)

    # A string that decodes to no locator is still refused, and the message does not echo it - the
    # locator of an S3 destination carries a secret access key. The client prints the query it sent
    # after the message, so only what the server produced is inspected here.
    error = instance.query_and_get_error(
        "CREATE DATABASE test_legacy_metadata_broken ENGINE = Backup('test_legacy_metadata_source', 'not a locator SEKRIT_LOCATOR')"
    )
    assert "BAD_ARGUMENTS" in error, error
    assert "SEKRIT_LOCATOR" not in error.split("\n(query:")[0], error

    instance.query("DROP DATABASE IF EXISTS test_legacy_metadata_view SYNC")
    instance.query("DROP DATABASE IF EXISTS test_legacy_metadata_source SYNC")
    cleanup_backup_files(instance)
