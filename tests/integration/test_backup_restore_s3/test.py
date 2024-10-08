import os
import uuid
from typing import Dict

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/disk_s3.xml",
        "configs/named_collection_s3_backups.xml",
        "configs/s3_settings.xml",
        "configs/blob_log.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=[
        "configs/zookeeper_retries.xml",
    ],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)


def setup_minio_users():
    # create 2 extra users with restricted access
    # miniorestricted1 - full access to bucket 'root', no access to other buckets
    # miniorestricted2 - full access to bucket 'root2', no access to other buckets
    # storage policy 'policy_s3_restricted' defines a policy for storing files inside bucket 'root' using 'miniorestricted1' user
    for user, bucket in [("miniorestricted1", "root"), ("miniorestricted2", "root2")]:
        print(
            cluster.exec_in_container(
                cluster.minio_docker_id,
                [
                    "mc",
                    "alias",
                    "set",
                    "root",
                    "http://minio1:9001",
                    "minio",
                    "minio123",
                ],
            )
        )
        policy = f"""
{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Effect": "Allow",
      "Principal": {{
        "AWS": [
          "*"
        ]
      }},
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads"
      ],
      "Resource": [
        "arn:aws:s3:::{bucket}"
      ]
    }},
    {{
      "Effect": "Allow",
      "Principal": {{
        "AWS": [
          "*"
        ]
      }},
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:ListMultipartUploadParts",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::{bucket}/*"
      ]
    }}
  ]
}}"""

        cluster.exec_in_container(
            cluster.minio_docker_id,
            ["bash", "-c", f"cat >/tmp/{bucket}_policy.json <<EOL{policy}"],
        )
        cluster.exec_in_container(
            cluster.minio_docker_id, ["cat", f"/tmp/{bucket}_policy.json"]
        )
        print(
            cluster.exec_in_container(
                cluster.minio_docker_id,
                ["mc", "admin", "user", "add", "root", user, "minio123"],
            )
        )
        print(
            cluster.exec_in_container(
                cluster.minio_docker_id,
                [
                    "mc",
                    "admin",
                    "policy",
                    "create",
                    "root",
                    f"{bucket}only",
                    f"/tmp/{bucket}_policy.json",
                ],
            )
        )
        print(
            cluster.exec_in_container(
                cluster.minio_docker_id,
                [
                    "mc",
                    "admin",
                    "policy",
                    "attach",
                    "root",
                    f"{bucket}only",
                    "--user",
                    user,
                ],
            )
        )

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disk_s3_restricted_user.xml"),
        "/etc/clickhouse-server/config.d/disk_s3_restricted_user.xml",
    )
    node.start_clickhouse()


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        setup_minio_users()
        yield
    finally:
        cluster.shutdown()


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"backup{backup_id_counter}"


def get_events_for_query(query_id: str) -> Dict[str, int]:
    events = TSV(
        node.query(
            f"""
            SYSTEM FLUSH LOGS;

            WITH arrayJoin(ProfileEvents) as pe
            SELECT pe.1, pe.2
            FROM system.query_log
            WHERE query_id = '{query_id}'
            """
        )
    )
    result = {
        event: int(value)
        for event, value in [line.split("\t") for line in events.lines]
    }
    result["query_id"] = query_id
    return result


def format_settings(settings):
    if not settings:
        return ""
    return "SETTINGS " + ",".join(f"{k}={v}" for k, v in settings.items())


def check_backup_and_restore(
    storage_policy,
    backup_destination,
    size=1000,
    backup_settings=None,
    restore_settings=None,
    insert_settings=None,
    optimize_table=True,
):
    optimize_table_query = "OPTIMIZE TABLE data FINAL;" if optimize_table else ""

    node.query(
        f"""
    DROP TABLE IF EXISTS data SYNC;
    CREATE TABLE data (key Int, value String, array Array(String)) Engine=MergeTree() ORDER BY tuple() SETTINGS storage_policy='{storage_policy}';
    INSERT INTO data SELECT * FROM generateRandom('key Int, value String, array Array(String)') LIMIT {size} {format_settings(insert_settings)};
    {optimize_table_query}
    """
    )

    try:
        backup_query_id = uuid.uuid4().hex
        node.query(
            f"BACKUP TABLE data TO {backup_destination} {format_settings(backup_settings)}",
            query_id=backup_query_id,
        )
        restore_query_id = uuid.uuid4().hex
        node.query(
            f"""
            RESTORE TABLE data AS data_restored FROM {backup_destination} {format_settings(restore_settings)};
            """,
            query_id=restore_query_id,
        )
        node.query(
            """
            SELECT throwIf(
                (SELECT count(), sum(sipHash64(*)) FROM data) !=
                (SELECT count(), sum(sipHash64(*)) FROM data_restored),
                'Data does not matched after BACKUP/RESTORE'
        );
        """
        )
        return [
            get_events_for_query(backup_query_id),
            get_events_for_query(restore_query_id),
        ]
    finally:
        node.query(
            """
            DROP TABLE data SYNC;
            DROP TABLE IF EXISTS data_restored SYNC;
            """
        )


def check_system_tables(backup_query_id=None):
    disks = [
        tuple(disk.split("\t"))
        for disk in node.query(
            "SELECT name, type, object_storage_type, metadata_type FROM system.disks"
        ).split("\n")
        if disk
    ]
    expected_disks = (
        ("default", "Local", "None", "None"),
        ("disk_s3", "ObjectStorage", "S3", "Local"),
        ("disk_s3_cache", "ObjectStorage", "S3", "Local"),
        ("disk_s3_other_bucket", "ObjectStorage", "S3", "Local"),
        ("disk_s3_plain", "ObjectStorage", "S3", "Plain"),
        ("disk_s3_plain_rewritable", "ObjectStorage", "S3", "PlainRewritable"),
        ("disk_s3_restricted_user", "ObjectStorage", "S3", "Local"),
    )
    assert len(expected_disks) == len(disks)
    for expected_disk in expected_disks:
        if expected_disk not in disks:
            raise AssertionError(f"Missed {expected_disk} in {disks}")

    if backup_query_id is not None:
        blob_storage_log = node.query(
            f"SELECT count() FROM system.blob_storage_log WHERE query_id = '{backup_query_id}' AND error = '' AND event_type = 'Upload'"
        ).strip()
        assert int(blob_storage_log) >= 1, node.query(
            "SELECT * FROM system.blob_storage_log FORMAT PrettyCompactMonoBlock"
        )


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


@pytest.mark.parametrize(
    "storage_policy, to_disk",
    [
        pytest.param(
            "policy_s3",
            "disk_s3_other_bucket",
            id="from_s3_to_s3",
        ),
        pytest.param(
            "policy_s3_other_bucket",
            "disk_s3",
            id="from_s3_to_s3_other_bucket",
        ),
    ],
)
def test_backup_from_s3_to_s3_disk_native_copy(storage_policy, to_disk):
    backup_name = new_backup_name()
    backup_destination = f"Disk('{to_disk}', '{backup_name}')"
    (backup_events, restore_events) = check_backup_and_restore(
        storage_policy, backup_destination
    )

    assert backup_events["S3CopyObject"] > 0
    assert restore_events["S3CopyObject"] > 0


def test_backup_to_s3():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = (
        f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'minio123')"
    )
    (backup_events, _) = check_backup_and_restore(storage_policy, backup_destination)
    check_system_tables(backup_events["query_id"])


def test_backup_to_s3_named_collection():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3(named_collection_s3_backups, '{backup_name}')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_s3_multipart():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/multipart/{backup_name}', 'minio', 'minio123')"
    (backup_events, restore_events) = check_backup_and_restore(
        storage_policy,
        backup_destination,
        size=1000000,
    )
    assert node.contains_in_log(
        f"copyDataToS3File: Multipart upload has completed. Bucket: root, Key: data/backups/multipart/{backup_name}"
    )

    backup_query_id = backup_events["query_id"]
    blob_storage_log = node.query(
        f"SELECT countIf(event_type == 'MultiPartUploadCreate') * countIf(event_type == 'MultiPartUploadComplete') * countIf(event_type == 'MultiPartUploadWrite') "
        f"FROM system.blob_storage_log WHERE query_id = '{backup_query_id}' AND error = ''"
    ).strip()
    assert int(blob_storage_log) >= 1, node.query(
        "SELECT * FROM system.blob_storage_log FORMAT PrettyCompactMonoBlock"
    )

    s3_backup_events = (
        "WriteBufferFromS3Microseconds",
        "WriteBufferFromS3Bytes",
        "WriteBufferFromS3RequestsErrors",
    )
    s3_restore_events = (
        "ReadBufferFromS3Microseconds",
        "ReadBufferFromS3Bytes",
        "ReadBufferFromS3RequestsErrors",
    )

    objects = node.cluster.minio_client.list_objects(
        "root", f"data/backups/multipart/{backup_name}/"
    )
    backup_meta_size = 0
    for obj in objects:
        if ".backup" in obj.object_name:
            backup_meta_size = obj.size
            break
    backup_total_size = int(
        node.query(
            f"SELECT sum(total_size) FROM system.backups WHERE status = 'BACKUP_CREATED' AND name like '%{backup_name}%'"
        ).strip()
    )
    restore_total_size = int(
        node.query(
            f"SELECT sum(total_size) FROM system.backups WHERE status = 'RESTORED' AND name like '%{backup_name}%'"
        ).strip()
    )
    # backup
    # NOTE: ~35 bytes is used by .lock file, so set up 100 bytes to avoid flaky test
    assert (
        abs(
            backup_total_size
            - (backup_events["WriteBufferFromS3Bytes"] - backup_meta_size)
        )
        < 100
    )
    assert backup_events["WriteBufferFromS3Microseconds"] > 0
    assert "WriteBufferFromS3RequestsErrors" not in backup_events
    # restore
    assert (
        restore_events["ReadBufferFromS3Bytes"] - backup_meta_size == restore_total_size
    )
    assert restore_events["ReadBufferFromS3Microseconds"] > 0
    assert "ReadBufferFromS3RequestsErrors" not in restore_events


@pytest.mark.parametrize(
    "storage_policy",
    [
        "policy_s3",
        "policy_s3_other_bucket",
        "policy_s3_plain_rewritable",
    ],
)
def test_backup_to_s3_native_copy(storage_policy):
    backup_name = new_backup_name()
    backup_destination = (
        f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'minio123')"
    )
    (backup_events, restore_events) = check_backup_and_restore(
        storage_policy, backup_destination
    )
    # single part upload
    assert backup_events["S3CopyObject"] > 0
    assert restore_events["S3CopyObject"] > 0
    assert node.contains_in_log(
        f"copyS3File: Single operation copy has completed. Bucket: root, Key: data/backups/{backup_name}"
    )


def test_backup_to_s3_native_copy_multipart():
    storage_policy = "policy_s3"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/multipart/{backup_name}', 'minio', 'minio123')"
    (backup_events, restore_events) = check_backup_and_restore(
        storage_policy, backup_destination, size=1000000
    )
    # multi part upload
    assert backup_events["S3CreateMultipartUpload"] > 0
    assert restore_events["S3CreateMultipartUpload"] > 0
    assert node.contains_in_log(
        f"copyS3File: Multipart upload has completed. Bucket: root, Key: data/backups/multipart/{backup_name}/"
    )


def test_incremental_backup_append_table_def():
    backup_name = f"S3('http://minio1:9001/root/data/backups/{new_backup_name()}', 'minio', 'minio123')"

    node.query(
        "CREATE TABLE data (x UInt32, y String) Engine=MergeTree() ORDER BY y PARTITION BY x%10 SETTINGS storage_policy='policy_s3'"
    )

    node.query("INSERT INTO data SELECT number, toString(number) FROM numbers(100)")
    assert node.query("SELECT count(), sum(x) FROM data") == "100\t4950\n"

    node.query(f"BACKUP TABLE data TO {backup_name}")

    node.query("ALTER TABLE data MODIFY SETTING parts_to_throw_insert=100")

    incremental_backup_name = f"S3('http://minio1:9001/root/data/backups/{new_backup_name()}', 'minio', 'minio123')"

    node.query(
        f"BACKUP TABLE data TO {incremental_backup_name} SETTINGS base_backup = {backup_name}"
    )

    node.query("DROP TABLE data")
    node.query(f"RESTORE TABLE data FROM {incremental_backup_name}")

    assert node.query("SELECT count(), sum(x) FROM data") == "100\t4950\n"
    assert "parts_to_throw_insert = 100" in node.query("SHOW CREATE TABLE data")


@pytest.mark.parametrize(
    "in_cache_initially, allow_backup_read_cache, allow_s3_native_copy",
    [
        (False, True, False),
        (True, False, False),
        (True, True, False),
        (True, True, True),
    ],
)
def test_backup_with_fs_cache(
    in_cache_initially, allow_backup_read_cache, allow_s3_native_copy
):
    storage_policy = "policy_s3_cache"

    backup_name = new_backup_name()
    backup_destination = (
        f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'minio123')"
    )

    insert_settings = {
        "enable_filesystem_cache_on_write_operations": int(in_cache_initially)
    }

    backup_settings = {
        "read_from_filesystem_cache": int(allow_backup_read_cache),
        "allow_s3_native_copy": int(allow_s3_native_copy),
    }

    restore_settings = {"allow_s3_native_copy": int(allow_s3_native_copy)}

    backup_events, restore_events = check_backup_and_restore(
        storage_policy,
        backup_destination,
        size=10,
        insert_settings=insert_settings,
        optimize_table=False,
        backup_settings=backup_settings,
        restore_settings=restore_settings,
    )

    # print(f"backup_events = {backup_events}")
    # print(f"restore_events = {restore_events}")

    # BACKUP never updates the filesystem cache but it may read it if `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` allows that.
    if allow_backup_read_cache and in_cache_initially:
        assert backup_events["CachedReadBufferReadFromCacheBytes"] > 0
        assert not "CachedReadBufferReadFromSourceBytes" in backup_events
    elif allow_backup_read_cache:
        assert not "CachedReadBufferReadFromCacheBytes" in backup_events
        assert backup_events["CachedReadBufferReadFromSourceBytes"] > 0
    else:
        assert not "CachedReadBufferReadFromCacheBytes" in backup_events
        assert not "CachedReadBufferReadFromSourceBytes" in backup_events

    assert not "CachedReadBufferCacheWriteBytes" in backup_events
    assert not "CachedWriteBufferCacheWriteBytes" in backup_events

    # RESTORE doesn't use the filesystem cache during write operations.
    # However while attaching parts it may use the cache while reading such files as "columns.txt" or "checksums.txt" or "primary.idx",
    # see IMergeTreeDataPart::loadColumnsChecksumsIndexes()
    if "CachedReadBufferReadFromSourceBytes" in restore_events:
        assert (
            restore_events["CachedReadBufferReadFromSourceBytes"]
            == restore_events["CachedReadBufferCacheWriteBytes"]
        )

    assert not "CachedReadBufferReadFromCacheBytes" in restore_events

    # "format_version.txt" is written when a table is created,
    # see MergeTreeData::initializeDirectoriesAndFormatVersion()
    if "CachedWriteBufferCacheWriteBytes" in restore_events:
        assert restore_events["CachedWriteBufferCacheWriteBytes"] <= 1


def test_backup_to_zip():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.zip', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_tar():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.tar', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_tar_gz():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.tar.gz', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_tar_bz2():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.tar.bz2', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_tar_lzma():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.tar.lzma', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_tar_zst():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.tar.zst', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_backup_to_tar_xz():
    storage_policy = "default"
    backup_name = new_backup_name()
    backup_destination = f"S3('http://minio1:9001/root/data/backups/{backup_name}.tar.xz', 'minio', 'minio123')"
    check_backup_and_restore(storage_policy, backup_destination)


def test_user_specific_auth(start_cluster):
    def create_user(user):
        node.query(f"CREATE USER {user}")
        node.query(f"GRANT CURRENT GRANTS ON *.* TO {user}")

    create_user("superuser1")
    create_user("superuser2")
    create_user("regularuser")

    node.query("CREATE TABLE specific_auth (col UInt64) ENGINE=MergeTree ORDER BY col")
    node.query("INSERT INTO specific_auth VALUES (1)")

    def backup_restore(backup, user, should_fail, on_cluster=False, base_backup=None):
        on_cluster_clause = "ON CLUSTER 'cluster'" if on_cluster else ""
        base_backup = (
            f" SETTINGS base_backup = {base_backup}" if base_backup is not None else ""
        )
        backup_query = (
            f"BACKUP TABLE specific_auth {on_cluster_clause} TO {backup} {base_backup}"
        )
        restore_query = f"RESTORE TABLE specific_auth {on_cluster_clause} FROM {backup}"

        if should_fail:
            assert "Access" in node.query_and_get_error(backup_query, user=user)
        else:
            node.query(backup_query, user=user)
            node.query("DROP TABLE specific_auth SYNC")
            node.query(restore_query, user=user)

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup1/')",
        user=None,
        should_fail=True,
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup1/')",
        user="regularuser",
        should_fail=True,
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup1/')",
        user="superuser1",
        should_fail=False,
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup2/')",
        user="superuser2",
        should_fail=False,
    )

    assert "Access" in node.query_and_get_error(
        "RESTORE TABLE specific_auth FROM S3('http://minio1:9001/root/data/backups/limited/backup1/')",
        user="regularuser",
    )

    node.query("INSERT INTO specific_auth VALUES (2)")

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup1_inc/')",
        user="regularuser",
        should_fail=True,
        base_backup="S3('http://minio1:9001/root/data/backups/limited/backup1/')",
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup1_inc/')",
        user="superuser1",
        should_fail=False,
        base_backup="S3('http://minio1:9001/root/data/backups/limited/backup1/')",
    )

    assert "Access" in node.query_and_get_error(
        "RESTORE TABLE specific_auth FROM S3('http://minio1:9001/root/data/backups/limited/backup1_inc/')",
        user="regularuser",
    )

    assert "Access Denied" in node.query_and_get_error(
        "SELECT * FROM s3('http://minio1:9001/root/data/backups/limited/backup1/*', 'RawBLOB')",
        user="regularuser",
    )

    node.query(
        "SELECT * FROM s3('http://minio1:9001/root/data/backups/limited/backup1/*', 'RawBLOB')",
        user="superuser1",
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup3/')",
        user="regularuser",
        should_fail=True,
        on_cluster=True,
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup3/')",
        user="superuser1",
        should_fail=False,
        on_cluster=True,
    )

    assert "Access Denied" in node.query_and_get_error(
        "RESTORE TABLE specific_auth ON CLUSTER 'cluster' FROM S3('http://minio1:9001/root/data/backups/limited/backup3/')",
        user="regularuser",
    )

    node.query("INSERT INTO specific_auth VALUES (3)")

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup3_inc/')",
        user="regularuser",
        should_fail=True,
        on_cluster=True,
        base_backup="S3('http://minio1:9001/root/data/backups/limited/backup3/')",
    )

    backup_restore(
        "S3('http://minio1:9001/root/data/backups/limited/backup3_inc/')",
        user="superuser1",
        should_fail=False,
        on_cluster=True,
        base_backup="S3('http://minio1:9001/root/data/backups/limited/backup3/')",
    )

    assert "Access Denied" in node.query_and_get_error(
        "RESTORE TABLE specific_auth ON CLUSTER 'cluster' FROM S3('http://minio1:9001/root/data/backups/limited/backup3_inc/')",
        user="regularuser",
    )

    assert "Access Denied" in node.query_and_get_error(
        "SELECT * FROM s3('http://minio1:9001/root/data/backups/limited/backup3/*', 'RawBLOB')",
        user="regularuser",
    )

    node.query(
        "SELECT * FROM s3('http://minio1:9001/root/data/backups/limited/backup3/*', 'RawBLOB')",
        user="superuser1",
    )

    assert "Access Denied" in node.query_and_get_error(
        "SELECT * FROM s3Cluster(cluster, 'http://minio1:9001/root/data/backups/limited/backup3/*', 'RawBLOB')",
        user="regularuser",
    )

    node.query("DROP TABLE IF EXISTS test.specific_auth")


def test_backup_to_s3_different_credentials():
    storage_policy = "policy_s3_restricted"

    def check_backup_restore(allow_s3_native_copy):
        backup_name = new_backup_name()
        backup_destination = f"S3('http://minio1:9001/root2/data/backups/{backup_name}', 'miniorestricted2', 'minio123')"
        settings = {"allow_s3_native_copy": allow_s3_native_copy}
        (backup_events, _) = check_backup_and_restore(
            storage_policy,
            backup_destination,
            backup_settings=settings,
            restore_settings=settings,
        )
        check_system_tables(backup_events["query_id"])

    check_backup_restore(False)
    check_backup_restore(True)
