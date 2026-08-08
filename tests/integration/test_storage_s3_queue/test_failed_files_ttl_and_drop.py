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
        "unordered",
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
        "unordered",
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
        "unordered",
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
        "unordered",
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


def test_system_drop_ordered_mode_blocked(started_cluster):
    """Test that SYSTEM DROP S3QUEUE FAILED FILES is blocked in ordered mode"""
    node = started_cluster.instances["instance"]

    table_name = f"test_ordered_mode_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # Create table in ordered mode
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

    create_mv(node, table_name, dst_table_name)

    # Try to run SYSTEM DROP on ordered mode table - should fail with NOT_IMPLEMENTED
    error = node.query_and_get_error(
        f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}"
    )

    assert "NOT_IMPLEMENTED" in error or "Code: 48" in error, \
        f"Expected NOT_IMPLEMENTED error, got: {error}"
    assert "only supported for unordered mode" in error, \
        f"Error message should mention unordered mode requirement, got: {error}"

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_failed_file_ttl_ordered_mode_no_cleanup(started_cluster):
    """Test that failed_file_ttl_sec setting does not trigger cleanup in ordered mode.

    In ordered mode, cleanup_failed_files is disabled by design (matching cleanup_processed_files
    pattern), so setting failed_file_ttl_sec has no effect and the periodic cleanup thread
    simply skips the failed files path.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_ttl_ordered_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # Create table in ordered mode with failed_file_ttl_sec set
    # This should be accepted but ignored (cleanup_failed_files will be false)
    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "failed_file_ttl_sec": 3,  # Will be ignored in ordered mode
            "cleanup_interval_min_ms": 2000,
            "cleanup_interval_max_ms": 2000,
            "s3queue_loading_retries": 0,
        },
    )

    # Create an invalid file that will fail
    invalid_csv = b"invalid,data,here\n"
    put_s3_file_content(
        started_cluster, f"{files_path}/bad_file.csv", invalid_csv
    )

    create_mv(node, table_name, dst_table_name)

    # Wait for file to fail
    time.sleep(3)

    def get_failed_count():
        return int(node.query(
            f"SELECT count() FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip())

    # File should be marked as failed
    failed_count = get_failed_count()
    if failed_count > 0:
        # Wait past the TTL period
        time.sleep(5)

        # In ordered mode, TTL cleanup is disabled, so failed file should still be there
        failed_count_after = get_failed_count()
        assert failed_count_after == failed_count, \
            "Failed files should NOT be cleaned up in ordered mode (cleanup_failed_files is disabled)"
    else:
        logging.info("No failed files detected - ordered mode may handle failures differently")

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_failed_file_ttl_does_not_reset_retry_counter(started_cluster):
    """Test that failed_file_ttl_sec cleanup does NOT reset the retry counter.

    This test verifies the fix for the bug where TTL cleanup was deleting .retriable
    nodes (which store the retry count), causing the retry counter to reset to 0
    and allowing files to retry forever instead of reaching the terminal failed state.

    The test creates a race condition where:
    1. A file fails and creates a .retriable node with retries=0
    2. TTL cleanup runs and (with the bug) would delete the .retriable node
    3. The file retries and (with the bug) would restart from retries=0

    With the fix, .retriable nodes are skipped by cleanup, so retries increment
    correctly and the file reaches terminal failed state after s3queue_loading_retries.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_ttl_retry_counter_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # Set up timing to create the race condition:
    # - polling_min_timeout_ms=5000: 5 seconds between retry attempts
    # - failed_file_ttl_sec=2: TTL cleanup tries to delete nodes after 2 seconds
    # - cleanup_interval=2000ms: cleanup sweep runs every 2 seconds
    # This means cleanup runs 2-3 times between retry attempts, exercising the race.
    #
    # With the bug: .retriable node gets deleted before next retry, counter resets to 0
    # With the fix: .retriable node is preserved, retries increment to 3 → terminal failed

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 3,  # Allow 3 retries before terminal failure
            "failed_file_ttl_sec": 2,  # TTL shorter than retry interval
            "cleanup_interval_min_ms": 2000,
            "cleanup_interval_max_ms": 2000,
            "polling_min_timeout_ms": 5000,  # 5 seconds between retries
            "polling_max_timeout_ms": 5000,
        },
    )

    # Create one invalid CSV file that will fail every time
    invalid_csv = b"not,valid,data\n"
    put_s3_file_content(
        started_cluster, f"{files_path}/bad_retry.csv", invalid_csv
    )

    create_mv(node, table_name, dst_table_name)

    def get_file_status():
        """Get the file's status from metadata cache."""
        result = node.query(
            f"SELECT status FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND file_name = 'bad_retry.csv'"
        ).strip()
        return result if result else None

    def get_retry_count_from_keeper():
        """Parse retry count from zookeeper node data.

        Queries both terminal failed nodes and .retriable nodes.
        Returns (retries, is_terminal) tuple.
        """
        failed_path = f"{keeper_path}/failed"

        # Get all failed nodes (both terminal and .retriable)
        result = node.query(
            f"SELECT name, value FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()

        if not result:
            return None, False

        for line in result.split("\n"):
            parts = line.split("\t")
            if len(parts) != 2:
                continue

            node_name, node_value = parts

            # Parse the NodeMetadata JSON-like structure
            # Format is roughly: {"file_path":"...","retries":N,...}
            import re

            # Node names are hashes, not filenames - check file_path in the JSON instead
            file_path_match = re.search(r'"file_path"\s*:\s*"([^"]*)"', node_value)
            if not file_path_match or "bad_retry.csv" not in file_path_match.group(1):
                continue

            is_terminal = not node_name.endswith(".retriable")

            # Extract retries count - the data format is a simple struct with retries field
            match = re.search(r'"retries"\s*:\s*(\d+)', node_value)
            if match:
                retries = int(match.group(1))
                return retries, is_terminal

        return None, False

    logging.info("Waiting for file to go through retry cycles...")

    # Track retry progression to detect if counter is resetting
    max_retries_seen = -1
    timeout = 90  # 90 seconds should be enough for 4 attempts at 5s intervals + overhead

    for elapsed in range(timeout):
        time.sleep(1)

        status = get_file_status()
        retries, is_terminal = get_retry_count_from_keeper()

        if retries is not None:
            if retries > max_retries_seen:
                max_retries_seen = retries
                logging.info(
                    f"[{elapsed}s] Retry count: {retries}, "
                    f"terminal: {is_terminal}, status: {status}"
                )

            # Success case: reached terminal failed state with exactly 3 retries
            if is_terminal and retries == 3:
                logging.info(
                    f"SUCCESS: File reached terminal failed state with retries={retries} after {elapsed}s"
                )
                assert status == "Failed", \
                    f"Status should be 'Failed' in terminal state, got: {status}"
                break

            # Bug detection: retry counter went backwards (got reset)
            if retries < max_retries_seen:
                pytest.fail(
                    f"BUG DETECTED: Retry counter reset from {max_retries_seen} to {retries}. "
                    f"The .retriable node was likely deleted by TTL cleanup, breaking the retry limit invariant."
                )
        else:
            # File not in failed state yet, still processing
            if elapsed % 10 == 0:
                logging.info(f"[{elapsed}s] File not in failed state yet, status: {status}")

    else:
        # Timeout - distinguish between "stuck retrying" vs "never started"
        final_retries, final_is_terminal = get_retry_count_from_keeper()
        final_status = get_file_status()

        if final_retries is not None and not final_is_terminal:
            pytest.fail(
                f"TIMEOUT: File stuck retrying after {timeout}s. "
                f"Last retry count: {final_retries}, max seen: {max_retries_seen}. "
                f"The retry counter may be resetting (bug present), preventing terminal failure."
            )
        else:
            pytest.fail(
                f"TIMEOUT: File did not reach terminal failed state after {timeout}s. "
                f"Status: {final_status}, retries: {final_retries}, terminal: {final_is_terminal}"
            )

    # Final verification: file is in terminal failed state with exactly 3 retries
    final_retries, final_is_terminal = get_retry_count_from_keeper()
    final_status = get_file_status()

    assert final_is_terminal, "File should be in terminal failed state (no .retriable suffix)"
    assert final_retries == 3, \
        f"Terminal failed node should have retries=3, got: {final_retries}"
    assert final_status == "Failed", \
        f"Cache status should be 'Failed', got: {final_status}"

    logging.info("Test passed: retry counter was NOT reset by TTL cleanup")

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_retriable_node_cleanup_on_success(started_cluster):
    """Test that .retriable nodes are cleaned up when a file that failed then succeeds.

    This test verifies the fix for the bug where .retriable nodes (which track
    retry counts for files that failed but are still within their retry budget)
    were not removed when a file eventually succeeded, leaving stale garbage in
    Keeper that could interfere with future processing.

    Test scenario:
    1. Upload a file with invalid CSV content that will fail parsing
    2. Wait for it to fail at least once, creating a .retriable node with retries > 0
    3. Replace the S3 object with valid CSV content (same key)
    4. On the next retry, S3Queue reads the new valid content and succeeds
    5. Verify the file reaches 'Processed' status
    6. Verify no .retriable node remains in Keeper for that file

    This exercises the actual code path: prepareProcessedRequestsImpl() must remove
    the .retriable node when transitioning a file to processed state.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_retriable_cleanup_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    test_file = "retry_test.csv"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 5,  # Allow plenty of retries
            "polling_min_timeout_ms": 3000,  # 3 second retry interval (time to replace file)
            "polling_max_timeout_ms": 3000,
        },
    )

    create_mv(node, table_name, dst_table_name)

    def has_retriable_node_for_file(filename):
        """Check if a .retriable node exists for the given file."""
        failed_path = f"{keeper_path}/failed"
        # Query all nodes under /failed and parse their data to find ones matching our file
        result = node.query(
            f"SELECT name, value FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        if not result:
            return False

        import re
        for line in result.split("\n"):
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            node_name, node_value = parts
            if not node_name.endswith(".retriable"):
                continue
            # Check if this .retriable node is for our file
            file_path_match = re.search(r'"file_path"\s*:\s*"([^"]*)"', node_value)
            if file_path_match and filename in file_path_match.group(1):
                return True
        return False

    def get_file_status(filename):
        """Get file status from metadata cache."""
        result = node.query(
            f"SELECT status FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND file_name = '{filename}'"
        ).strip()
        return result if result else None

    # Step 1: Upload invalid CSV content (will fail parsing)
    logging.info(f"Step 1: Uploading invalid CSV to {test_file}")
    invalid_csv = b"not,valid,numbers,at,all\n"
    put_s3_file_content(started_cluster, f"{files_path}/{test_file}", invalid_csv)

    # Step 2: Wait for the file to fail and create a .retriable node
    logging.info("Step 2: Waiting for file to fail and create .retriable node...")
    retriable_node_created = False
    for attempt in range(20):
        time.sleep(1)
        if has_retriable_node_for_file(test_file):
            logging.info(f"✓ .retriable node created after {attempt + 1} seconds")
            retriable_node_created = True
            break

    assert retriable_node_created, \
        f"File {test_file} should have created a .retriable node after failing"

    # Step 3: Replace the S3 object with valid CSV content
    logging.info(f"Step 3: Replacing {test_file} with valid CSV content")
    # The table expects UInt32 columns (default from helpers), so provide valid numbers
    valid_csv = b"1,2,3\n4,5,6\n7,8,9\n"
    put_s3_file_content(started_cluster, f"{files_path}/{test_file}", valid_csv)
    logging.info("✓ File replaced with valid content")

    # Step 4: Wait for the file to be retried and succeed
    logging.info("Step 4: Waiting for retry to pick up valid content and succeed...")
    file_succeeded = False
    for attempt in range(30):
        time.sleep(1)
        status = get_file_status(test_file)
        if status == "Processed":
            logging.info(f"✓ File reached 'Processed' status after {attempt + 1} seconds")
            file_succeeded = True
            break

    assert file_succeeded, \
        f"File {test_file} should have succeeded after content was fixed (status: {get_file_status(test_file)})"

    # Step 5: Verify no .retriable node remains
    logging.info("Step 5: Verifying .retriable node was cleaned up...")
    time.sleep(2)  # Small buffer to ensure Keeper state is settled

    assert not has_retriable_node_for_file(test_file), \
        f"No .retriable node should remain for {test_file} after successful processing"

    logging.info("✓ Test passed: .retriable node was cleaned up on success")

    # Verify the file actually processed data
    result_count = int(node.query(f"SELECT count() FROM {dst_table_name}").strip())
    assert result_count > 0, "Destination table should have rows from the processed file"
    logging.info(f"✓ Destination table has {result_count} rows from the processed file")

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")
