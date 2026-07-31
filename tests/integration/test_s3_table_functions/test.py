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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        allow_anonymous_minio_reads(cluster)

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
    A 1200ms network delay must make the S3 write time out and raise.
    """

    # Make the S3 request timeout (not the connect timeout) the single failure mechanism:
    # disable adaptive timeouts and keep the connect timeout above the delay, so the write
    # can only fail via s3_request_timeout_ms. This exercises the send/receive idleness
    # timeout that applies to every attempt on both fresh and reused (pooled keep-alive)
    # connections, which is the path that was silently not timing out before.
    timeout_settings = {
        **settings,
        "s3_use_adaptive_timeouts": "0",
        "s3_connect_timeout_ms": "10000",
        "s3_request_timeout_ms": "500",
    }

    with PartitionManager() as pm:
        pm.add_network_delay(node, 1200)

        with pytest.raises(QueryRuntimeException, match="Timeout"):
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
                settings=timeout_settings,
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
        == "2\n"
    )
    assert (
        node.query(
            """
            SELECT id
            FROM lanceS3(nc_s3, filename = 'lance/pushdown.lance')
            WHERE event_time >= toDateTime64('2024-01-02 03:04:05.123', 3)
            ORDER BY id
            """
        )
        == "4\n5\n7\n8\n"
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
            id UInt64,
            name String,
            score Nullable(Int64)
        )
        ENGINE = LanceS3(nc_s3, filename = 'lance/basic.lance')
        """
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
            score Nullable(Int64)
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
                'minio',
                'ClickHouse_Minio_P@ssw0rd')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3(
                'http://minio1:9001/root/data/lance/basic.lance',
                access_key_id = 'minio',
                secret_access_key = 'ClickHouse_Minio_P@ssw0rd',
                region = 'us-east-1')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )

    node.query("DROP TABLE IF EXISTS lance_s3_explicit_credentials")
    node.query(
        """
        CREATE TABLE lance_s3_explicit_credentials
        ENGINE = LanceS3(
            'http://minio1:9001/root/data/lance/basic.lance',
            'minio',
            'ClickHouse_Minio_P@ssw0rd')
        """
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


def test_lance_s3_error_paths(started_cluster):
    skip_if_lance_s3_unavailable()

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)
    unsupported_prefix = "data/lance/extension_unsupported.lance"
    upload_lance_dataset_to_minio(
        started_cluster,
        unsupported_prefix,
        dataset_name="extension_unsupported.lance",
    )

    missing_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(nc_s3, filename = 'lance/missing.lance')
        """
    )
    assert "missing.lance" in missing_error or "not found" in missing_error.lower()

    credentials_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(
            'http://minio1:9001/root/data/lance/basic.lance',
            'minio',
            'wrong-secret')
        """
    )
    assert (
        "AccessDenied" in credentials_error
        or "forbidden" in credentials_error.lower()
        or "signature" in credentials_error.lower()
    )

    session_token_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(
            'http://minio1:9001/root/data/lance/basic.lance',
            access_key_id = 'minio',
            secret_access_key = 'ClickHouse_Minio_P@ssw0rd',
            session_token = 'session-token-that-minio-rejects')
        """
    )
    assert "S3_ERROR" in session_token_error or "Failed to get object info" in session_token_error

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

    unsupported_error = node.query_and_get_error(
        """
        SELECT *
        FROM lanceS3(nc_s3, filename = 'lance/extension_unsupported.lance')
        """
    )
    assert "BAD_ARGUMENTS" in unsupported_error
    assert "Unsupported Lance column `extension_value`" in unsupported_error
