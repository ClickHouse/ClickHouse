import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    put_s3_file_content,
    create_table,
    create_mv,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_failed_file_ttl_sec(started_cluster):
    """Test that failed files are automatically removed after TTL expires"""
    node = started_cluster.instances["instance"]

    table_name = f"test_failed_file_ttl_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # Short TTL for testing - 3 seconds
    ttl_sec = 3
    # Short cleanup interval - 2 seconds (default is 60 seconds)
    cleanup_interval_ms = 2000

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "failed_file_ttl_sec": ttl_sec,
            "cleanup_interval_min_ms": cleanup_interval_ms,
            "cleanup_interval_max_ms": cleanup_interval_ms,
            "s3queue_loading_retries": 0,  # Fail immediately without retries
        },
    )

    # Create one valid file to ensure the table is processing
    generate_random_files(
        started_cluster, files_path, 1, start_ind=0, row_num=1
    )

    # Create an invalid CSV file that will fail processing
    # The table expects UInt32 columns, so a string will cause parsing failure
    invalid_csv = b"invalid,data,here\n"
    put_s3_file_content(
        started_cluster, f"{files_path}/bad_file.csv", invalid_csv
    )

    create_mv(node, table_name, dst_table_name)

    def get_failed_files_from_cache():
        result = node.query(
            f"SELECT file_name FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip()
        return set(result.split("\n")) if result else set()

    def get_failed_files_from_keeper():
        """Query the actual Keeper /failed/ path to verify znodes exist"""
        failed_path = f"{keeper_path}/failed"
        result = node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return set(result.split("\n")) if result else set()

    # Wait for the bad file to be marked as failed (up to 60 seconds)
    for _ in range(60):
        failed_files = get_failed_files_from_cache()
        if "bad_file.csv" in failed_files:
            break
        time.sleep(1)

    assert "bad_file.csv" in get_failed_files_from_cache(), "File should be in failed cache"

    # Verify the failed znode exists in Keeper
    failed_znodes = get_failed_files_from_keeper()
    assert len(failed_znodes) > 0, "Failed znode should exist in Keeper"

    logging.info(f"Failed file detected. Waiting for TTL cleanup (TTL={ttl_sec}s)...")

    # Wait for TTL to expire and cleanup to run (TTL + buffer)
    # The cleanup runs periodically, so poll for up to 30 seconds
    ttl_cleanup_succeeded = False
    for attempt in range(30):
        time.sleep(1)
        if attempt >= ttl_sec + 3:  # Start checking after TTL + small buffer
            failed_files_after_ttl = get_failed_files_from_cache()
            failed_znodes_after_ttl = get_failed_files_from_keeper()

            if "bad_file.csv" not in failed_files_after_ttl and len(failed_znodes_after_ttl) == 0:
                logging.info(f"TTL cleanup succeeded after {attempt + 1} seconds")
                ttl_cleanup_succeeded = True
                break

    # Final verification
    assert ttl_cleanup_succeeded, \
        f"TTL cleanup failed after 30s: cache still has {get_failed_files_from_cache()}, " \
        f"keeper has {len(get_failed_files_from_keeper())} znodes"

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_system_drop_s3queue_failed_files_single(started_cluster):
    """Test SYSTEM DROP S3QUEUE FAILED FILES command with a single failed file"""
    node = started_cluster.instances["instance"]

    table_name = f"test_drop_failed_single_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,
        },
    )

    # Create one invalid file
    invalid_csv = b"not,valid,numbers\n"
    put_s3_file_content(
        started_cluster, f"{files_path}/failed_1.csv", invalid_csv
    )

    create_mv(node, table_name, dst_table_name)

    def get_failed_count():
        return int(node.query(
            f"SELECT count() FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip())

    def get_failed_znodes_count():
        failed_path = f"{keeper_path}/failed"
        result = node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    # Wait for the file to fail
    for _ in range(60):
        if get_failed_count() == 1:
            break
        time.sleep(1)

    assert get_failed_count() == 1, "Should have 1 failed file"
    assert get_failed_znodes_count() > 0, "Should have failed znodes in Keeper"

    # Run SYSTEM DROP command
    node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")

    # Verify all failed files are removed
    assert get_failed_count() == 0, "Failed files should be removed from cache"
    assert get_failed_znodes_count() == 0, "Failed znodes should be removed from Keeper"

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_system_drop_s3queue_failed_files_bulk(started_cluster):
    """Test SYSTEM DROP S3QUEUE FAILED FILES command with multiple failed files"""
    node = started_cluster.instances["instance"]

    table_name = f"test_drop_failed_bulk_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,
        },
    )

    # Create multiple invalid files
    invalid_csv = b"bad,bad,bad\n"
    num_failed_files = 5
    for i in range(num_failed_files):
        put_s3_file_content(
            started_cluster, f"{files_path}/failed_{i}.csv", invalid_csv
        )

    create_mv(node, table_name, dst_table_name)

    def get_failed_count():
        return int(node.query(
            f"SELECT count() FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip())

    def get_failed_znodes_count():
        failed_path = f"{keeper_path}/failed"
        result = node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    # Wait for all files to fail
    for _ in range(60):
        if get_failed_count() == num_failed_files:
            break
        time.sleep(1)

    assert get_failed_count() == num_failed_files, f"Should have {num_failed_files} failed files"
    assert get_failed_znodes_count() > 0, "Should have failed znodes in Keeper"

    # Run SYSTEM DROP command once - should remove all failed files
    node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")

    # Verify all failed files are removed in one operation
    assert get_failed_count() == 0, "All failed files should be removed from cache"
    assert get_failed_znodes_count() == 0, "All failed znodes should be removed from Keeper"

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_system_drop_s3queue_failed_files_idempotent(started_cluster):
    """Test that SYSTEM DROP S3QUEUE FAILED FILES is idempotent - succeeds on empty failed set"""
    node = started_cluster.instances["instance"]

    table_name = f"test_drop_idempotent_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
        },
    )

    # Create only valid files - no failures
    generate_random_files(
        started_cluster, files_path, 2, start_ind=0, row_num=1
    )

    create_mv(node, table_name, dst_table_name)

    # Wait for files to be processed successfully
    time.sleep(3)

    def get_failed_count():
        return int(node.query(
            f"SELECT count() FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip())

    # Verify no failed files
    assert get_failed_count() == 0, "Should have no failed files"

    # Run SYSTEM DROP command on empty failed set - should succeed without error
    node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")

    # Run it again to test true idempotency
    node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")

    # Still no failed files
    assert get_failed_count() == 0, "Should still have no failed files"

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")
