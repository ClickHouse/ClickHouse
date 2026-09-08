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
