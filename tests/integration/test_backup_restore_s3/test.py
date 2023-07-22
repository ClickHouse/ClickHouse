import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/disk_s3.xml",
        "configs/named_collection_s3_backups.xml",
        "configs/s3_settings.xml",
    ],
    user_configs=[
        "configs/zookeeper_retries.xml",
    ],
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"backup{backup_id_counter}"


def check_backup_and_restore(storage_policy, backup_destination, size=1000):
    node.query(
        f"""
    DROP TABLE IF EXISTS data NO DELAY;
    CREATE TABLE data (key Int, value String, array Array(String)) Engine=MergeTree() ORDER BY tuple() SETTINGS storage_policy='{storage_policy}';
    INSERT INTO data SELECT * FROM generateRandom('key Int, value String, array Array(String)') LIMIT {size};
    BACKUP TABLE data TO {backup_destination};
    RESTORE TABLE data AS data_restored FROM {backup_destination};
    SELECT throwIf(
        (SELECT count(), sum(sipHash64(*)) FROM data) !=
        (SELECT count(), sum(sipHash64(*)) FROM data_restored),
        'Data does not matched after BACKUP/RESTORE'
    );
    DROP TABLE data NO DELAY;
    DROP TABLE data_restored NO DELAY;
    """
    )


def check_system_tables():
    disks = [
        tuple(disk.split("\t"))
        for disk in node.query("SELECT name, type FROM system.disks").split("\n")
        if disk
    ]
    expected_disks = (
        ("default", "local"),
        ("disk_s3", "s3"),
        ("disk_s3_other_bucket", "s3"),
        ("disk_s3_plain", "s3_plain"),
    )
    assert len(expected_disks) == len(disks)
    for expected_disk in expected_disks:
        if expected_disk not in disks:
            raise AssertionError(f"Missed {expected_disk} in {disks}")


@pytest.mark.parametrize(
    "storage_policy, to_disk",
    [
        pytest.param(
            "default",
            "default",
            id="from_local_to_local",
        ),
        pytest.param(
            "policy_s3",
            "default",
            id="from_s3_to_local",
        ),
        pytest.param(
            "default",
            "disk_s3",
            id="from_local_to_s3",
        ),
        pytest.param(
            "policy_s3",
            "disk_s3_plain",
            id="from_s3_to_s3_plain",
        ),
        pytest.param(
            "default",
            "disk_s3_plain",
            id="from_local_to_s3_plain",
        ),
    ],
)
def test_backup_to_disk(storage_policy, to_disk):
    backup_name = new_backup_name()
    backup_destination = f"Disk('{to_disk}', '{backup_name}')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_s3():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = (
        f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'minio123')"
    )
    check_backup_and_restore(storage_policy, backup_destination)
    check_system_tables()


def test_backup_to_s3_named_collection():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3(named_collection_s3_backups, '{backup_name}')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_s3_multipart():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/multipart/{backup_name}', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination, size=1000000)
    assert node.contains_in_log(
        f"copyDataToS3File: Multipart upload has completed. Bucket: root, Key: data/backups/multipart/{backup_name}"
    )


def test_backup_to_s3_native_copy():
    storage_policy = "policy_s3"
    backup_name = new_backup_name()
    backup_destination = (
        f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'minio123')"
    )
    check_backup_and_restore(storage_policy, backup_destination)
    assert node.contains_in_log("BackupImpl.*using native copy")
    assert node.contains_in_log("copyS3FileToDisk.*using native copy")
    assert node.contains_in_log(
        f"copyS3File: Single operation copy has completed. Bucket: root, Key: data/backups/{backup_name}"
    )


def test_backup_to_s3_native_copy_other_bucket():
    storage_policy = "policy_s3_other_bucket"
    backup_name = new_backup_name()
    backup_destination = (
        f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'minio123')"
    )
    check_backup_and_restore(storage_policy, backup_destination)
    assert node.contains_in_log("BackupImpl.*using native copy")
    assert node.contains_in_log("copyS3FileToDisk.*using native copy")
    assert node.contains_in_log(
        f"copyS3File: Single operation copy has completed. Bucket: root, Key: data/backups/{backup_name}"
    )


def test_backup_to_s3_native_copy_multipart():
    storage_policy = "policy_s3"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/multipart/{backup_name}', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination, size=1000000)
    assert node.contains_in_log("BackupImpl.*using native copy")
    assert node.contains_in_log("copyS3FileToDisk.*using native copy")
    assert node.contains_in_log(
        f"copyS3File: Multipart upload has completed. Bucket: root, Key: data/backups/multipart/{backup_name}/"
    )
