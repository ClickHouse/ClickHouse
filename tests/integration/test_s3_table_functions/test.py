import concurrent.futures
import io
import json
import logging
import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/minio.xml",
    ],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    with_minio=True,
)
node_with_environment_credentials = cluster.add_instance(
    "node_with_environment_credentials",
    main_configs=[
        "configs/config.d/minio.xml",
    ],
    user_configs=[
        "configs/users.d/users.xml",
        "configs/users.d/allow_server_credentials.xml",
    ],
    env_variables={
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "ClickHouse_Minio_P@ssw0rd",
    },
    with_minio=True,
)

settings = {
    "s3_max_connections": "1",
    "max_insert_threads": "1",
    "s3_truncate_on_insert": "1",
    "s3_min_upload_part_size": "33554432",
}

LANCE_S3_TEST_ACCESS_KEY = "lances3explicit"
LANCE_S3_TEST_SECRET_KEY = "LanceS3ExplicitSecret"

lance_s3_query_parameters = {
    "param_access_key": LANCE_S3_TEST_ACCESS_KEY,
    "param_secret_key": LANCE_S3_TEST_SECRET_KEY,
}


def upload_lance_dataset_to_minio(started_cluster, remote_prefix, dataset_name="basic.lance"):
    local_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../queries/0_stateless/data_lance",
            dataset_name,
        )
    )
    for root, _, files in os.walk(local_path):
        for filename in files:
            local_file = os.path.join(root, filename)
            relative_path = os.path.relpath(local_file, local_path)
            remote_path = os.path.join(remote_prefix, relative_path)
            started_cluster.minio_client.fput_object(
                bucket_name=started_cluster.minio_bucket,
                object_name=remote_path,
                file_path=local_file,
            )


def upload_lance_files_to_minio(
    started_cluster, remote_prefix, dataset_name, relative_paths
):
    local_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../queries/0_stateless/data_lance",
            dataset_name,
        )
    )
    for relative_path in relative_paths:
        started_cluster.minio_client.fput_object(
            bucket_name=started_cluster.minio_bucket,
            object_name=os.path.join(remote_prefix, relative_path),
            file_path=os.path.join(local_path, relative_path),
        )


def set_lance_latest_version(started_cluster, remote_prefix, version):
    payload = json.dumps({"version": version}, separators=(",", ":")).encode()
    started_cluster.minio_client.put_object(
        bucket_name=started_cluster.minio_bucket,
        object_name=f"{remote_prefix}/_versions/latest_version_hint.json",
        data=io.BytesIO(payload),
        length=len(payload),
    )


def remove_minio_prefix(started_cluster, remote_prefix):
    for item in started_cluster.minio_client.list_objects(
        started_cluster.minio_bucket,
        prefix=f"{remote_prefix.rstrip('/')}/",
        recursive=True,
    ):
        started_cluster.minio_client.remove_object(
            started_cluster.minio_bucket, item.object_name
        )


def skip_if_lance_s3_unavailable(instance=node):
    if instance.query("SELECT count() FROM system.table_functions WHERE name = 'lanceS3'") == "0\n":
        pytest.skip("lanceS3 table function is not available in this build")


def allow_anonymous_minio_reads(started_cluster):
    bucket_read_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::root/*",
            },
        ],
    }
    started_cluster.minio_client.set_bucket_policy(
        started_cluster.minio_bucket,
        json.dumps(bucket_read_policy),
    )


def create_lance_s3_test_user(started_cluster):
    minio_container_id = started_cluster.get_container_id(started_cluster.minio_host)
    started_cluster.exec_in_container(
        minio_container_id,
        [
            "sh",
            "-c",
            (
                "mc alias set local http://127.0.0.1:9001 "
                '"$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
                f"mc admin user add local {LANCE_S3_TEST_ACCESS_KEY} "
                f"{LANCE_S3_TEST_SECRET_KEY} >/dev/null && "
                f"mc admin policy attach local readwrite "
                f"--user {LANCE_S3_TEST_ACCESS_KEY} >/dev/null"
            ),
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        allow_anonymous_minio_reads(cluster)
        create_lance_s3_test_user(cluster)

        yield cluster
    finally:
        logging.info("Stopping cluster")
        cluster.shutdown()
        logging.info("Cluster stopped")


def test_s3_table_functions(started_cluster):
    """
    Simple test to check s3 table function functionalities
    """
    node.query(
        """
            INSERT INTO FUNCTION s3
                (
                    nc_s3,
                    filename = 'test_file.tsv.gz',
                    format = 'TSV',
                    structure = 'number UInt64',
                    compression_method = 'gz'
                )
            SELECT * FROM numbers(1000000)
        """,
        settings=settings,
    )

    assert (
        node.query(
            """
            SELECT count(*) FROM s3
            (
                nc_s3,
                filename = 'test_file.tsv.gz',
                format = 'TSV',
                structure = 'number UInt64',
                compression_method = 'gz'
            );
        """
        )
        == "1000000\n"
    )


def test_s3_table_functions_timeouts(started_cluster):
    """
    Test with timeout limit of 1200ms.
    This should raise an Exception and pass.
    """

    with PartitionManager() as pm:
        pm.add_network_delay(node, 1200)

        with pytest.raises(QueryRuntimeException):
            node.query(
                """
                INSERT INTO FUNCTION s3
                    (
                        nc_s3,
                        filename = 'test_file.tsv.gz',
                        format = 'TSV',
                        structure = 'number UInt64',
                        compression_method = 'gz'
                    )
                SELECT * FROM numbers(1000000)
            """,
                settings=settings,
            )


def test_lance_s3_table_function(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3(nc_s3, filename = 'lance/basic.lance')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )


def test_lance_s3_data_types(started_cluster):
    skip_if_lance_s3_unavailable()

    for dataset_name in ["rich_types.lance", "map.lance"]:
        upload_lance_dataset_to_minio(
            started_cluster,
            f"data/lance/{dataset_name}",
            dataset_name=dataset_name,
        )

    assert (
        node.query(
            """
            SELECT
                count() = 3,
                count(string_value) = 2,
                count(decimal_value) = 2,
                sum(length(array_value)) = 3,
                countIf(toTypeName(array_value) = 'Array(Nullable(Int32))') = 3
            FROM lanceS3(nc_s3, filename = 'lance/rich_types.lance')
            """
        )
        == "1\t1\t1\t1\t1\n"
    )

    assert (
        node.query(
            """
            SELECT countIf(length(m) >= 0) = count()
            FROM lanceS3(nc_s3, filename = 'lance/map.lance')
            """
        )
        == "1\n"
    )


def test_lance_s3_table_engine(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    node.query("DROP TABLE IF EXISTS lance_s3_basic")
    node.query(
        """
        CREATE TABLE lance_s3_basic
        ENGINE = LanceS3(nc_s3, filename = 'lance/basic.lance')
        """
    )

    assert node.query("SELECT count() FROM lance_s3_basic") == "3\n"
    assert (
        node.query("SELECT id, name FROM lance_s3_basic ORDER BY id")
        == "1\ta\n2\tb\n3\tc\n"
    )

    node.query("DROP TABLE lance_s3_basic")


def test_lance_s3_query_pins_dataset_version(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/versions.lance"
    version_1_files = [
        "data/101110110111010110111111ca31c045e49cba0cf12615f6a3.lance",
        "_transactions/0-9e7942fd-8397-4e82-b2be-d4688d628047.txn",
        "_versions/18446744073709551614.manifest",
    ]
    version_2_data_and_transaction = [
        "data/010000010001111010111000942f4c499e86a6951e1774c786.lance",
        "_transactions/1-b97f116d-e6ae-4cc4-ba38-2b525803283d.txn",
    ]
    version_2_manifest = "_versions/18446744073709551613.manifest"
    version_2_manifest_object = f"{remote_prefix}/{version_2_manifest}"

    for item in started_cluster.minio_client.list_objects(
        started_cluster.minio_bucket,
        prefix=f"{remote_prefix}/",
        recursive=True,
    ):
        started_cluster.minio_client.remove_object(
            started_cluster.minio_bucket, item.object_name
        )

    upload_lance_files_to_minio(
        started_cluster,
        remote_prefix,
        "versions.lance",
        version_1_files,
    )
    set_lance_latest_version(started_cluster, remote_prefix, 1)
    assert version_2_manifest_object not in {
        item.object_name
        for item in started_cluster.minio_client.list_objects(
            started_cluster.minio_bucket,
            prefix=f"{remote_prefix}/_versions/",
            recursive=True,
        )
    }

    failpoint = "lance_metadata_iterate_pause"
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

    def run_query(query_id):
        return node.query(
            """
            SELECT
                count(),
                sum(id),
                uniqExact(_data_lake_snapshot_version),
                min(_data_lake_snapshot_version)
            FROM lanceS3(nc_s3, filename = 'lance/versions.lance')
            """,
            query_id=query_id,
            timeout=60,
        )

    query_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    wait_future = query_executor.submit(
        lambda: node.query(
            f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE",
            timeout=30,
        )
    )
    query_future = query_executor.submit(run_query, "lance_snapshot_consistency_pinned")

    try:
        wait_future.result(timeout=30)
        upload_lance_files_to_minio(
            started_cluster,
            remote_prefix,
            "versions.lance",
            version_2_data_and_transaction,
        )
        upload_lance_files_to_minio(
            started_cluster,
            remote_prefix,
            "versions.lance",
            [version_2_manifest],
        )
        set_lance_latest_version(started_cluster, remote_prefix, 2)
        assert version_2_manifest_object in {
            item.object_name
            for item in started_cluster.minio_client.list_objects(
                started_cluster.minio_bucket,
                prefix=f"{remote_prefix}/_versions/",
                recursive=True,
            )
        }
        node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")

        assert query_future.result(timeout=60) == "3\t6\t1\t1\n"
        assert run_query("lance_snapshot_consistency_latest") == "4\t10\t1\t2\n"
    finally:
        try:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        finally:
            query_executor.shutdown(wait=True)


def test_lance_s3_pinned_version_deleted_error(started_cluster):
    """After analysis pins a version, deleting that version's objects must fail cleanly (no hang)."""
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/versions_deleted.lance"
    version_1_files = [
        "data/101110110111010110111111ca31c045e49cba0cf12615f6a3.lance",
        "_transactions/0-9e7942fd-8397-4e82-b2be-d4688d628047.txn",
        "_versions/18446744073709551614.manifest",
    ]
    version_1_data_object = (
        f"{remote_prefix}/data/101110110111010110111111ca31c045e49cba0cf12615f6a3.lance"
    )
    version_1_manifest_object = f"{remote_prefix}/_versions/18446744073709551614.manifest"

    remove_minio_prefix(started_cluster, remote_prefix)
    upload_lance_files_to_minio(
        started_cluster,
        remote_prefix,
        "versions.lance",
        version_1_files,
    )
    set_lance_latest_version(started_cluster, remote_prefix, 1)

    failpoint = "lance_metadata_iterate_pause"
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

    def run_query():
        return node.query_and_get_error(
            """
            SELECT id, name
            FROM lanceS3(nc_s3, filename = 'lance/versions_deleted.lance')
            ORDER BY id
            """,
            query_id="lance_pinned_version_deleted",
            timeout=60,
        )

    query_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    wait_future = query_executor.submit(
        lambda: node.query(
            f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE",
            timeout=30,
        )
    )
    query_future = query_executor.submit(run_query)

    try:
        wait_future.result(timeout=30)
        # Snapshot is already pinned to v1; remove the pinned version's storage objects.
        for object_name in (version_1_data_object, version_1_manifest_object):
            started_cluster.minio_client.remove_object(
                started_cluster.minio_bucket, object_name
            )
        node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")

        deleted_error = query_future.result(timeout=60)
        # Prefer structured codes from the Lance FFI mapping (NotFound / S3 / corrupt).
        assert any(
            code in deleted_error
            for code in (
                "FILE_DOESNT_EXIST",
                "S3_ERROR",
                "INCORRECT_DATA",
                "CANNOT_OPEN_FILE",
            )
        ), deleted_error
    finally:
        try:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        finally:
            query_executor.shutdown(wait=True)
            remove_minio_prefix(started_cluster, remote_prefix)


def test_lance_s3_cold_hot_reread(started_cluster):
    """Cold then warm re-read of the same S3 dataset must return identical results."""
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic_reread.lance"
    remove_minio_prefix(started_cluster, remote_prefix)
    upload_lance_dataset_to_minio(
        started_cluster,
        remote_prefix,
        dataset_name="basic.lance",
    )

    query = """
        SELECT id, name, score
        FROM lanceS3(nc_s3, filename = 'lance/basic_reread.lance')
        ORDER BY id
        """
    cold = node.query(query, query_id="lance_s3_cold_read")
    hot = node.query(query, query_id="lance_s3_hot_read")
    assert cold == hot
    assert cold == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"


def test_lance_s3_pushdown_queries(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/pushdown.lance"
    upload_lance_dataset_to_minio(
        started_cluster,
        remote_prefix,
        dataset_name="pushdown.lance",
    )

    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id = 1 OR id = 3
            ORDER BY id
            """
        )
        == "1\n3\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id IN (1, 3, 5)
            ORDER BY id
            """
        )
        == "1\n3\n5\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE score IS NULL
            ORDER BY id
            """
        )
        == "2\n5\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE score IS NOT NULL
            ORDER BY id
            """
        )
        == "1\n3\n4\n6\n7\n8\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE event_date = toDate('2024-01-02')
            ORDER BY id
            """
        )
        == "2\n7\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE event_time >= toDateTime64('2024-01-02 03:04:05.123', 3)
            ORDER BY id
            """,
            settings={"session_timezone": "UTC"},
        )
        == "2\n4\n5\n7\n"
    )
    assert (
        node.query(
            """
            SELECT count()
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE score = CAST(NULL, 'Nullable(Float64)')
            """
        )
        == "0\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE lower(name) = 'x'
            ORDER BY id
            """
        )
        == "4\n7\n"
    )
    # Partial AND: id is pushable, lower(name) is residual-only.
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id IN (4, 7) AND lower(name) = 'x'
            ORDER BY id
            """
        )
        == "4\n7\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id = 2 AND lower(name) = 'nope'
            """
        )
        == ""
    )
    # LIMIT without WHERE (size only; any rows are fine).
    assert (
        node.query(
            """
            SELECT count()
            FROM (
                SELECT id
                FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
                LIMIT 2
            )
            """
        )
        == "2\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id IN (1, 3, 5, 7)
            ORDER BY id
            LIMIT 2
            """
        )
        == "1\n3\n"
    )
    assert (
        node.query(
            """
            SELECT count()
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id = 1 OR id = 3
            """
        )
        == "2\n"
    )
    assert (
        node.query(
            """
            SELECT count()
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE id IN (1, 3, 5)
            """
        )
        == "3\n"
    )
    assert (
        node.query(
            """
            SELECT count()
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE score IS NULL
            """
        )
        == "2\n"
    )
    assert (
        node.query(
            """
            SELECT id, _path != ''
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            ORDER BY id
            LIMIT 2
            """
        )
        == "1\t1\n2\t1\n"
    )
    assert (
        node.query(
            """
            SELECT _data_lake_snapshot_version = _data_lake_snapshot_version
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            LIMIT 3
            """
        )
        == "1\n1\n1\n"
    )
    assert (
        node.query(
            """
            SELECT count(), countIf(_path != '')
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            """
        )
        == "8\t8\n"
    )
    assert (
        node.query(
            """
            SELECT count(), uniqExact(_data_lake_snapshot_version)
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            """
        )
        == "8\t1\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3('lance/pushdown.lance', SETTINGS disk = 'lance_s3_disk')
            WHERE score IS NULL
            ORDER BY id
            """
        )
        == "2\n5\n"
    )


def test_lance_s3_explicit_schema(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    node.query("DROP TABLE IF EXISTS lance_s3_explicit")
    node.query(
        """
        CREATE TABLE lance_s3_explicit
        (
            id Int32,
            name String,
            score Nullable(Float32)
        )
        ENGINE = LanceS3(nc_s3, filename = 'lance/basic.lance')
        """
    )
    assert (
        node.query(
            "SELECT toTypeName(id), toTypeName(score) FROM lance_s3_explicit LIMIT 1"
        )
        == "Int32\tNullable(Float32)\n"
    )
    assert (
        node.query("SELECT id, name, score FROM lance_s3_explicit ORDER BY id")
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )
    node.query("DROP TABLE lance_s3_explicit")

    error = node.query_and_get_error(
        """
        CREATE TABLE lance_s3_mismatch
        (
            id String,
            name String,
            score Nullable(Float32)
        )
        ENGINE = LanceS3(nc_s3, filename = 'lance/basic.lance')
        """
    )
    assert "BAD_ARGUMENTS" in error
    assert "incompatible type for column `id`" in error


def test_lance_s3_explicit_credentials(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3(
                'http://minio1:9001/root/data/lance/basic.lance',
                {access_key:String},
                {secret_key:String})
            ORDER BY id
            """,
            settings=lance_s3_query_parameters,
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3(
                'http://minio1:9001/root/data/lance/basic.lance',
                access_key_id = {access_key:String},
                secret_access_key = {secret_key:String},
                region = 'us-east-1')
            ORDER BY id
            """,
            settings=lance_s3_query_parameters,
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )

    node.query("DROP TABLE IF EXISTS lance_s3_explicit_credentials")
    node.query(
        """
        CREATE TABLE lance_s3_explicit_credentials
        ENGINE = LanceS3(
            'http://minio1:9001/root/data/lance/basic.lance',
            {access_key:String},
            {secret_key:String})
        """,
        settings=lance_s3_query_parameters,
    )
    assert (
        node.query("SELECT id, name, score FROM lance_s3_explicit_credentials ORDER BY id")
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )
    node.query("DROP TABLE lance_s3_explicit_credentials")


def test_lance_s3_no_sign_named_collection(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3(nc_s3_no_sign, filename = 'lance/basic.lance')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )


def test_lance_s3_environment_credentials_named_collection(started_cluster):
    skip_if_lance_s3_unavailable(node_with_environment_credentials)

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    assert (
        node_with_environment_credentials.query(
            """
            SELECT id, name, score
            FROM lanceS3(nc_s3_env, filename = 'lance/basic.lance')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )


def test_lance_s3_disk_setting(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3('lance/basic.lance', SETTINGS disk = 'lance_s3_disk')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )

    node.query("DROP TABLE IF EXISTS lance_s3_disk")
    node.query(
        """
        CREATE TABLE lance_s3_disk
        ENGINE = LanceS3('lance/basic.lance')
        SETTINGS disk = 'lance_s3_disk'
        """
    )
    assert node.query("SELECT count() FROM lance_s3_disk") == "3\n"
    node.query("DROP TABLE lance_s3_disk")


def test_lance_s3_missing_dataset_error(started_cluster):
    skip_if_lance_s3_unavailable()

    missing_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(nc_s3, filename = 'lance/missing.lance')
        """
    )
    assert "FILE_DOESNT_EXIST" in missing_error
    assert "missing.lance" in missing_error or "not found" in missing_error.lower()


def test_lance_s3_invalid_uri_error(started_cluster):
    skip_if_lance_s3_unavailable()

    invalid_uri_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3('http://[invalid', 'minio', 'secret')
        """
    )
    assert "POCO_EXCEPTION" in invalid_uri_error
    assert "Bad URI syntax" in invalid_uri_error


def test_lance_s3_invalid_credentials_error(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    credentials_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(
            'http://minio1:9001/root/data/lance/basic.lance',
            'minio',
            'wrong-secret')
        """
    )
    assert "ACCESS_DENIED" in credentials_error


def test_lance_s3_invalid_session_token_error(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    session_token_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(
            'http://minio1:9001/root/data/lance/basic.lance',
            access_key_id = {access_key:String},
            secret_access_key = {secret_key:String},
            session_token = {session_token:String})
        """,
        settings={
            **lance_s3_query_parameters,
            "param_session_token": "session-token-that-minio-rejects",
        },
    )
    assert "ACCESS_DENIED" in session_token_error


def test_lance_s3_corrupt_manifest_error(started_cluster):
    skip_if_lance_s3_unavailable()

    corrupt_prefix = "data/lance/corrupt.lance"
    upload_lance_dataset_to_minio(started_cluster, corrupt_prefix)
    corrupt_manifest = next(
        item.object_name
        for item in started_cluster.minio_client.list_objects(
            bucket_name=started_cluster.minio_bucket,
            prefix=f"{corrupt_prefix}/_versions/",
            recursive=True,
        )
        if item.object_name.endswith(".manifest")
    )
    manifest_response = started_cluster.minio_client.get_object(
        bucket_name=started_cluster.minio_bucket,
        object_name=corrupt_manifest,
    )
    try:
        corrupt_payload = bytearray(manifest_response.read())
    finally:
        manifest_response.close()
        manifest_response.release_conn()
    assert len(corrupt_payload) > 4
    corrupt_payload[4] = 0
    started_cluster.minio_client.put_object(
        bucket_name=started_cluster.minio_bucket,
        object_name=corrupt_manifest,
        data=io.BytesIO(corrupt_payload),
        length=len(corrupt_payload),
    )

    corrupt_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(nc_s3, filename = 'lance/corrupt.lance')
        """
    )
    assert "INCORRECT_DATA" in corrupt_error


def test_lance_s3_disallowed_disk_error(started_cluster):
    skip_if_lance_s3_unavailable()

    disk_error = node.query_and_get_error(
        """
        CREATE TABLE lance_s3_non_s3_disk
        ENGINE = LanceS3('lance/basic.lance')
        SETTINGS disk = 'default'
        """
    )
    assert "BAD_ARGUMENTS" in disk_error
    assert "Unsupported disk type for LanceS3" in disk_error

    table_function_disk_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3('lance/basic.lance', SETTINGS disk = 'default')
        """
    )
    assert "BAD_ARGUMENTS" in table_function_disk_error
    assert "Unsupported disk type for lanceS3" in table_function_disk_error


def test_lance_s3_unsupported_type_error(started_cluster):
    skip_if_lance_s3_unavailable()

    unsupported_prefix = "data/lance/extension_unsupported.lance"
    upload_lance_dataset_to_minio(
        started_cluster,
        unsupported_prefix,
        dataset_name="extension_unsupported.lance",
    )

    unsupported_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(nc_s3, filename = 'lance/extension_unsupported.lance')
        """
    )
    assert "BAD_ARGUMENTS" in unsupported_error
    assert "Unsupported Lance column `extension_value`" in unsupported_error
