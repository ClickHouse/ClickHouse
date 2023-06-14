#!/usr/bin/env python3

import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock


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
        """
    )

    broken_s3.setup_fake_puts(1)

    error = node.query_and_get_error(
        "INSERT INTO s3_upload_after_check_works VALUES (1, 'Hello')"
    )

    assert "Code: 499" in error, error
    assert "Immediately after upload" in error, error
    assert "suddenly disappeared" in error, error


def get_counters(node, query_id, log_type="ExceptionWhileProcessing"):
    node.query("SYSTEM FLUSH LOGS")
    return [
        int(x)
        for x in node.query(
            f"""
                SELECT
                    ProfileEvents['S3CreateMultipartUpload'],
                    ProfileEvents['S3UploadPart'],
                    ProfileEvents['S3WriteRequestsErrors']
                FROM system.query_log
                WHERE query_id='{query_id}'
                    AND type='{log_type}'
                """
        ).split()
        if x
    ]


#  Add "lz4" compression method in the list after https://github.com/ClickHouse/ClickHouse/issues/50975 is fixed
@pytest.mark.parametrize(
    "compression", ["none", "gzip", "br", "xz", "zstd", "bz2", "deflate"]
)
def test_upload_s3_fail_create_multi_part_upload(cluster, broken_s3, compression):
    node = cluster.instances["node"]

    broken_s3.setup_error_at_create_multi_part_upload()

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

    count_create_multi_part_uploads, count_upload_parts, count_s3_errors = get_counters(
        node, insert_query_id
    )
    assert count_create_multi_part_uploads == 1
    assert count_upload_parts == 0
    assert count_s3_errors == 1


#  Add "lz4" compression method in the list after https://github.com/ClickHouse/ClickHouse/issues/50975 is fixed
@pytest.mark.parametrize(
    "compression", ["none", "gzip", "br", "xz", "zstd", "bz2", "deflate"]
)
def test_upload_s3_fail_upload_part_when_multi_part_upload(
    cluster, broken_s3, compression
):
    node = cluster.instances["node"]

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_error_at_part_upload(count=1, after=2)

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

    count_create_multi_part_uploads, count_upload_parts, count_s3_errors = get_counters(
        node, insert_query_id
    )
    assert count_create_multi_part_uploads == 1
    assert count_upload_parts >= 2
    assert (
        count_s3_errors == 2
    )  # the second is cancel multipart upload, s3_mock just redirects this request
