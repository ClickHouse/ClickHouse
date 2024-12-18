#!/usr/bin/env python3

import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock
from helpers.test_tools import assert_eq_with_retry


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage_conf.xml",
            ],
            user_configs=[
                "configs/setting.xml",
                "configs/s3_retries.xml",
            ],
            with_minio=True,
        )
        cluster.add_instance(
            "node_with_inf_s3_retries",
            main_configs=[
                "configs/storage_conf.xml",
            ],
            user_configs=[
                "configs/setting.xml",
                "configs/inf_s3_retries.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def init_broken_s3(cluster):
    yield start_s3_mock(cluster, "broken_s3", "8083")


@pytest.fixture(scope="function")
def broken_s3(init_broken_s3):
    init_broken_s3.reset()
    yield init_broken_s3


def test_upload_after_check_works(cluster, broken_s3):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_upload_after_check_works (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS
            storage_policy='broken_s3'
        """
    )

    broken_s3.setup_fake_puts(1)

    error = node.query_and_get_error(
        "INSERT INTO s3_upload_after_check_works VALUES (1, 'Hello')"
    )

    assert "Code: 499" in error, error
    assert "Immediately after upload" in error, error
    assert "suddenly disappeared" in error, error


def get_multipart_counters(node, query_id, log_type="ExceptionWhileProcessing"):
    node.query("SYSTEM FLUSH LOGS")
    return [
        int(x)
        for x in node.query(
            f"""
                SELECT
                    ProfileEvents['S3CreateMultipartUpload'],
                    ProfileEvents['S3UploadPart'],
                    ProfileEvents['S3WriteRequestsErrors'],
                FROM system.query_log
                WHERE query_id='{query_id}'
                    AND type='{log_type}'
                """
        ).split()
        if x
    ]


def get_put_counters(node, query_id, log_type="ExceptionWhileProcessing"):
    node.query("SYSTEM FLUSH LOGS")
    return [
        int(x)
        for x in node.query(
            f"""
                SELECT
                    ProfileEvents['S3PutObject'],
                    ProfileEvents['S3WriteRequestsErrors'],
                FROM system.query_log
                WHERE query_id='{query_id}'
                    AND type='{log_type}'
                """
        ).split()
        if x
    ]


@pytest.mark.parametrize(
    "compression", ["none", "gzip", "br", "xz", "zstd", "bz2", "deflate", "lz4"]
)
def test_upload_s3_fail_create_multi_part_upload(cluster, broken_s3, compression):
    node = cluster.instances["node"]

    broken_s3.setup_at_create_multi_part_upload()

    insert_query_id = f"INSERT_INTO_TABLE_FUNCTION_FAIL_CREATE_MPU_{compression}"
    error = node.query_and_get_error(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_upload_s3_fail_create_multi_part_upload',
                'minio', 'minio123',
                'CSV', auto, '{compression}'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 100000000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=100
        """,
        query_id=insert_query_id,
    )

    assert "Code: 499" in error, error
    assert "mock s3 injected error" in error, error

    create_multipart, upload_parts, s3_errors = get_multipart_counters(
        node, insert_query_id
    )
    assert create_multipart == 1
    assert upload_parts == 0
    assert s3_errors == 1


@pytest.mark.parametrize(
    "compression", ["none", "gzip", "br", "xz", "zstd", "bz2", "deflate", "lz4"]
)
def test_upload_s3_fail_upload_part_when_multi_part_upload(
    cluster, broken_s3, compression
):
    node = cluster.instances["node"]

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_part_upload(count=1, after=2)

    insert_query_id = f"INSERT_INTO_TABLE_FUNCTION_FAIL_UPLOAD_PART_{compression}"
    error = node.query_and_get_error(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_upload_s3_fail_upload_part_when_multi_part_upload',
                'minio', 'minio123',
                'CSV', auto, '{compression}'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 100000000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=100
        """,
        query_id=insert_query_id,
    )

    assert "Code: 499" in error, error
    assert "mock s3 injected error" in error, error

    create_multipart, upload_parts, s3_errors = get_multipart_counters(
        node, insert_query_id
    )
    assert create_multipart == 1
    assert upload_parts >= 2
    assert s3_errors >= 2


def test_when_s3_connection_refused_is_retried(cluster, broken_s3):
    node = cluster.instances["node"]

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_part_upload(count=3, after=2, action="connection_refused")

    insert_query_id = f"INSERT_INTO_TABLE_FUNCTION_CONNECTION_REFUSED_RETRIED"
    node.query(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_when_s3_connection_refused_at_write_retried',
                'minio', 'minio123',
                'CSV', auto, 'none'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 1000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=100,
            s3_check_objects_after_upload=0
        """,
        query_id=insert_query_id,
    )

    create_multipart, upload_parts, s3_errors = get_multipart_counters(
        node, insert_query_id, log_type="QueryFinish"
    )
    assert create_multipart == 1
    assert upload_parts == 39
    assert s3_errors == 3

    broken_s3.setup_at_part_upload(count=1000, after=2, action="connection_refused")
    insert_query_id = f"INSERT_INTO_TABLE_FUNCTION_CONNECTION_REFUSED_RETRIED_1"
    error = node.query_and_get_error(
        f"""
            INSERT INTO
                TABLE FUNCTION s3(
                    'http://resolver:8083/root/data/test_when_s3_connection_refused_at_write_retried',
                    'minio', 'minio123',
                    'CSV', auto, 'none'
                )
            SELECT
                *
            FROM system.numbers
            LIMIT 1000
            SETTINGS
                s3_max_single_part_upload_size=100,
                s3_min_upload_part_size=100,
                s3_check_objects_after_upload=0
            """,
        query_id=insert_query_id,
    )

    assert "Code: 499" in error, error
    assert (
        "Poco::Exception. Code: 1000, e.code() = 111, Connection refused" in error
    ), error


@pytest.mark.parametrize("send_something", [True, False])
def test_when_s3_connection_reset_by_peer_at_upload_is_retried(
    cluster, broken_s3, send_something
):
    node = cluster.instances["node"]

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_part_upload(
        count=3,
        after=2,
        action="connection_reset_by_peer",
        action_args=["1"] if send_something else ["0"],
    )

    insert_query_id = (
        f"TEST_WHEN_S3_CONNECTION_RESET_BY_PEER_AT_UPLOAD_{send_something}"
    )
    node.query(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_when_s3_connection_reset_by_peer_at_upload_is_retried',
                'minio', 'minio123',
                'CSV', auto, 'none'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 1000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=100,
            s3_check_objects_after_upload=0
        """,
        query_id=insert_query_id,
    )

    create_multipart, upload_parts, s3_errors = get_multipart_counters(
        node, insert_query_id, log_type="QueryFinish"
    )

    assert create_multipart == 1
    assert upload_parts == 39
    assert s3_errors == 3

    broken_s3.setup_at_part_upload(
        count=1000,
        after=2,
        action="connection_reset_by_peer",
        action_args=["1"] if send_something else ["0"],
    )
    insert_query_id = (
        f"TEST_WHEN_S3_CONNECTION_RESET_BY_PEER_AT_UPLOAD_{send_something}_1"
    )
    error = node.query_and_get_error(
        f"""
               INSERT INTO
                   TABLE FUNCTION s3(
                       'http://resolver:8083/root/data/test_when_s3_connection_reset_by_peer_at_upload_is_retried',
                       'minio', 'minio123',
                       'CSV', auto, 'none'
                   )
               SELECT
                   *
               FROM system.numbers
               LIMIT 1000
               SETTINGS
                   s3_max_single_part_upload_size=100,
                   s3_min_upload_part_size=100,
                   s3_check_objects_after_upload=0
               """,
        query_id=insert_query_id,
    )

    assert "Code: 1000" in error, error
    assert (
        "DB::Exception: Connection reset by peer." in error
        or "DB::Exception: Poco::Exception. Code: 1000, e.code() = 104, Connection reset by peer"
        in error
    ), error


@pytest.mark.parametrize("send_something", [True, False])
def test_when_s3_connection_reset_by_peer_at_create_mpu_retried(
    cluster, broken_s3, send_something
):
    node = cluster.instances["node"]

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_create_multi_part_upload(
        count=3,
        after=0,
        action="connection_reset_by_peer",
        action_args=["1"] if send_something else ["0"],
    )

    insert_query_id = (
        f"TEST_WHEN_S3_CONNECTION_RESET_BY_PEER_AT_MULTIPARTUPLOAD_{send_something}"
    )
    node.query(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_when_s3_connection_reset_by_peer_at_create_mpu_retried',
                'minio', 'minio123',
                'CSV', auto, 'none'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 1000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=100,
            s3_check_objects_after_upload=0
        """,
        query_id=insert_query_id,
    )

    create_multipart, upload_parts, s3_errors = get_multipart_counters(
        node, insert_query_id, log_type="QueryFinish"
    )

    assert create_multipart == 1
    assert upload_parts == 39
    assert s3_errors == 3

    broken_s3.setup_at_create_multi_part_upload(
        count=1000,
        after=0,
        action="connection_reset_by_peer",
        action_args=["1"] if send_something else ["0"],
    )

    insert_query_id = (
        f"TEST_WHEN_S3_CONNECTION_RESET_BY_PEER_AT_MULTIPARTUPLOAD_{send_something}_1"
    )
    error = node.query_and_get_error(
        f"""
               INSERT INTO
                   TABLE FUNCTION s3(
                       'http://resolver:8083/root/data/test_when_s3_connection_reset_by_peer_at_create_mpu_retried',
                       'minio', 'minio123',
                       'CSV', auto, 'none'
                   )
               SELECT
                   *
               FROM system.numbers
               LIMIT 1000
               SETTINGS
                   s3_max_single_part_upload_size=100,
                   s3_min_upload_part_size=100,
                   s3_check_objects_after_upload=0
               """,
        query_id=insert_query_id,
    )

    assert "Code: 1000" in error, error
    assert (
        "DB::Exception: Connection reset by peer." in error
        or "DB::Exception: Poco::Exception. Code: 1000, e.code() = 104, Connection reset by peer"
        in error
    ), error


def test_when_s3_broken_pipe_at_upload_is_retried(cluster, broken_s3):
    node = cluster.instances["node"]

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_part_upload(
        count=3,
        after=2,
        action="broken_pipe",
    )

    insert_query_id = f"TEST_WHEN_S3_BROKEN_PIPE_AT_UPLOAD"
    node.query(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_when_s3_broken_pipe_at_upload_is_retried',
                'minio', 'minio123',
                'CSV', auto, 'none'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 1000000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=1000000,
            s3_check_objects_after_upload=0
        """,
        query_id=insert_query_id,
    )

    create_multipart, upload_parts, s3_errors = get_multipart_counters(
        node, insert_query_id, log_type="QueryFinish"
    )

    assert create_multipart == 1
    assert upload_parts == 7
    assert s3_errors == 3

    broken_s3.setup_at_part_upload(
        count=1000,
        after=2,
        action="broken_pipe",
    )
    insert_query_id = f"TEST_WHEN_S3_BROKEN_PIPE_AT_UPLOAD_1"
    error = node.query_and_get_error(
        f"""
               INSERT INTO
                   TABLE FUNCTION s3(
                       'http://resolver:8083/root/data/test_when_s3_broken_pipe_at_upload_is_retried',
                       'minio', 'minio123',
                       'CSV', auto, 'none'
                   )
               SELECT
                   *
               FROM system.numbers
               LIMIT 1000000
               SETTINGS
                   s3_max_single_part_upload_size=100,
                   s3_min_upload_part_size=1000000,
                   s3_check_objects_after_upload=0
               """,
        query_id=insert_query_id,
    )

    assert "Code: 1000" in error, error
    assert (
        "DB::Exception: Poco::Exception. Code: 1000, e.code() = 32, I/O error: Broken pipe"
        in error
    ), error


def test_query_is_canceled_with_inf_retries(cluster, broken_s3):
    node = cluster.instances["node_with_inf_s3_retries"]

    broken_s3.setup_at_part_upload(
        count=10000000,
        after=2,
        action="connection_refused",
    )

    insert_query_id = f"TEST_QUERY_IS_CANCELED_WITH_INF_RETRIES"
    request = node.get_query_request(
        f"""
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test_query_is_canceled_with_inf_retries',
                'minio', 'minio123',
                'CSV', auto, 'none'
            )
        SELECT
            *
        FROM system.numbers
        LIMIT 1000000
        SETTINGS
            s3_max_single_part_upload_size=100,
            s3_min_upload_part_size=10000,
            s3_check_objects_after_upload=0
        """,
        query_id=insert_query_id,
    )

    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.processes WHERE query_id='{insert_query_id}'",
        "1",
    )

    assert_eq_with_retry(
        node,
        f"SELECT ProfileEvents['S3WriteRequestsErrors'] > 10 FROM system.processes WHERE query_id='{insert_query_id}'",
        "1",
        retry_count=12,
        sleep_time=10,
    )

    node.query(f"KILL QUERY WHERE query_id = '{insert_query_id}' ASYNC")

    # no more than 2 minutes
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.processes WHERE query_id='{insert_query_id}'",
        "0",
        retry_count=120,
        sleep_time=1,
    )


@pytest.mark.parametrize("node_name", ["node", "node_with_inf_s3_retries"])
def test_adaptive_timeouts(cluster, broken_s3, node_name):
    node = cluster.instances[node_name]

    broken_s3.setup_fake_puts(part_length=1)
    broken_s3.setup_slow_answers(
        timeout=5,
        count=1000000,
    )

    insert_query_id = f"TEST_ADAPTIVE_TIMEOUTS_{node_name}"
    node.query(
        f"""
            INSERT INTO
                TABLE FUNCTION s3(
                    'http://resolver:8083/root/data/adaptive_timeouts',
                    'minio', 'minio123',
                    'CSV', auto, 'none'
                )
            SELECT
                *
            FROM system.numbers
            LIMIT 1
            SETTINGS
                s3_request_timeout_ms=30000,
                s3_check_objects_after_upload=0
            """,
        query_id=insert_query_id,
    )

    broken_s3.reset()

    put_objects, s3_errors = get_put_counters(
        node, insert_query_id, log_type="QueryFinish"
    )

    assert put_objects == 1

    s3_use_adaptive_timeouts = node.query(
        f"""
        SELECT
            value
        FROM system.settings
        WHERE
            name='s3_use_adaptive_timeouts'
        """
    ).strip()

    if node_name == "node_with_inf_s3_retries":
        # first 2 attempts failed
        assert s3_use_adaptive_timeouts == "1"
        assert s3_errors == 1
    else:
        assert s3_use_adaptive_timeouts == "0"
        assert s3_errors == 0
