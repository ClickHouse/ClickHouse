import concurrent.futures
import json
import logging
import threading
import time
import uuid

import pytest
from kazoo.exceptions import NodeExistsError

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    generate_random_files,
    put_s3_file_content,
    create_table,
    create_mv,
)


# The lock value a manual drop writes is `manual_drop_failed:<command_id>`, and a waiter binds to that
# id rather than to the lock node, so it can follow the command across the retries it may make. Tests
# that play the winner by hand must therefore write a lock value of the same shape and publish markers
# carrying the same id.
TEST_DROP_COMMAND_ID = "11111111-2222-3333-4444-555555555555"
TEST_DROP_LOCK_VALUE = f"manual_drop_failed:{TEST_DROP_COMMAND_ID}".encode()
# A second, unrelated command. Used where a test hands the lock on to someone else: the point of those
# tests is that the waiter must NOT follow a different command, which only holds if the id differs.
OTHER_DROP_COMMAND_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
OTHER_DROP_LOCK_VALUE = f"manual_drop_failed:{OTHER_DROP_COMMAND_ID}".encode()


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
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )
        # Second replica, required by the `cluster` entry in remote_servers.xml
        # for the ON CLUSTER tests. Shares Keeper and MinIO with `instance`.
        cluster.add_instance(
            "instance2",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/zookeeper.xml",
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_failed_files_ttl_sec(started_cluster):
    """Test that failed files are automatically removed after TTL expires"""
    node = started_cluster.instances["instance"]

    table_name = f"test_failed_files_ttl_{uuid.uuid4().hex[:8]}"
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
            "failed_files_ttl_sec": ttl_sec,
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


def test_tracked_file_ttl_sec_does_not_expire_failed_files(started_cluster):
    """`tracked_file_ttl_sec` is the retention of the processed set and must not touch the failed set.

    Regression for the failed-set cleanup being driven by the processed-set knobs: the `/failed` sweep
    used to be enabled by `hasTrackedFilesLimit()` and handed `tracked_files_ttl_sec` as its TTL, so a
    table with `failed_files_ttl_sec = 0` still had its terminal failures expired on the processed set's
    schedule. Here only `tracked_file_ttl_sec` is set, so nothing may expire the failed marker.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_tracked_ttl_keeps_failed_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # Short enough that several cleanup runs pass while the test waits.
    tracked_file_ttl_sec = 3
    cleanup_interval_ms = 2000

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            # The processed-set knobs, both set. `tracked_files_limit = 0` leaves the count cap off, so
            # this table's only retention control is a TTL that belongs to `/processed`.
            "tracked_files_limit": 0,
            "tracked_file_ttl_sec": tracked_file_ttl_sec,
            # The failed-set knob, explicitly off: no time-based expiry of terminal failures.
            "failed_files_ttl_sec": 0,
            "cleanup_interval_min_ms": cleanup_interval_ms,
            "cleanup_interval_max_ms": cleanup_interval_ms,
            "s3queue_loading_retries": 0,  # Fail terminally on the first attempt.
        },
    )

    put_s3_file_content(
        started_cluster, f"{files_path}/bad_file.csv", b"invalid,data,here\n"
    )

    create_mv(node, table_name, dst_table_name)

    def terminal_failed_znodes():
        """Terminal `/failed/<hash>` children, excluding the `.retriable` retry-state markers."""
        result = node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/failed'"
        ).strip()
        names = result.split("\n") if result else []
        return {name for name in names if not name.endswith(".retriable")}

    for _ in range(60):
        if terminal_failed_znodes():
            break
        time.sleep(1)

    before = terminal_failed_znodes()
    assert len(before) == 1, f"Expected one terminal failed znode, got {before}"

    # Well past the processed-set TTL, and long enough for several cleanup runs at a 2s interval.
    time.sleep(tracked_file_ttl_sec + 4 * cleanup_interval_ms / 1000)

    after = terminal_failed_znodes()
    assert after == before, (
        f"`tracked_file_ttl_sec` expired a terminal failed file: {before} -> {after}. "
        f"Only `failed_files_ttl_sec` may expire the failed set."
    )
    assert (
        node.query(
            f"SELECT count() FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip()
        == "1"
    ), "The cache should still report the file as failed"

    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_tracked_files_limit_still_caps_the_failed_set(started_cluster):
    """`tracked_files_limit` remains a count cap on the failed set, as documented.

    The companion to `test_tracked_file_ttl_sec_does_not_expire_failed_files`: decoupling the two TTLs
    deliberately left the count cap alone, so `failed_files_ttl_sec = 0` means "no time-based expiry",
    not "keep every failure forever". Pinning that here keeps the setting's documentation honest.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_failed_count_cap_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    tracked_files_limit = 1
    # Deliberately long: the first sweep is scheduled *after* one interval
    # (`ObjectStorageQueueMetadata::startup` -> `scheduleAfter`), so both files are guaranteed to have
    # failed before any trimming happens. With a short interval the sweep can run while only the first
    # file has failed, and the test would then see one node and "pass" without ever having two.
    cleanup_interval_ms = 30000

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "tracked_files_limit": tracked_files_limit,
            "tracked_file_ttl_sec": 0,
            "failed_files_ttl_sec": 0,
            "cleanup_interval_min_ms": cleanup_interval_ms,
            "cleanup_interval_max_ms": cleanup_interval_ms,
            "s3queue_loading_retries": 0,
        },
    )

    for name in ["bad_one.csv", "bad_two.csv"]:
        put_s3_file_content(
            started_cluster, f"{files_path}/{name}", b"invalid,data,here\n"
        )

    create_mv(node, table_name, dst_table_name)

    def terminal_failed_znodes():
        result = node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{keeper_path}/failed'"
        ).strip()
        names = result.split("\n") if result else []
        return {name for name in names if not name.endswith(".retriable")}

    # Precondition, and the whole point of the test: the failed set must actually exceed the cap
    # before the sweep runs. Without this the test passes vacuously - one file failed, the count
    # already equals the cap, and nothing was ever trimmed.
    over_cap = False
    for _ in range(60):
        if len(terminal_failed_znodes()) > tracked_files_limit:
            over_cap = True
            break
        time.sleep(1)

    assert over_cap, (
        f"Both files should have failed terminally before the first sweep, "
        f"got {terminal_failed_znodes()}"
    )

    # Now the sweep trims back to the cap.
    converged = False
    for _ in range(90):
        time.sleep(1)
        if len(terminal_failed_znodes()) == tracked_files_limit:
            converged = True
            break

    assert converged, (
        f"Expected the failed set to be capped at {tracked_files_limit}, "
        f"got {terminal_failed_znodes()}"
    )

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


def test_failed_files_ttl_ordered_mode_no_cleanup(started_cluster):
    """Test that failed_files_ttl_sec setting does not trigger cleanup in ordered mode.

    In ordered mode, cleanup_failed_files is disabled by design (matching cleanup_processed_files
    pattern), so setting failed_files_ttl_sec has no effect and the periodic cleanup thread
    simply skips the failed files path.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_ttl_ordered_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # Create table in ordered mode with failed_files_ttl_sec set
    # This should be accepted but ignored (cleanup_failed_files will be false)
    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "failed_files_ttl_sec": 3,  # Will be ignored in ordered mode
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

    def get_failed_count():
        return int(node.query(
            f"SELECT count() FROM system.s3queue_metadata_cache "
            f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
        ).strip())

    # Wait for file to fail (up to 60 seconds)
    for _ in range(60):
        if get_failed_count() > 0:
            break
        time.sleep(1)

    # Assert file is marked as failed
    failed_count = get_failed_count()
    assert failed_count > 0, "Invalid file should be marked as Failed"

    if True:  # Always run the TTL check now that we confirmed failure
        # Wait past the TTL period
        time.sleep(5)

        # In ordered mode, TTL cleanup is disabled, so failed file should still be there
        failed_count_after = get_failed_count()
        assert failed_count_after == failed_count, \
            "Failed files should NOT be cleaned up in ordered mode (cleanup_failed_files is disabled)"

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_failed_files_ttl_does_not_reset_retry_counter(started_cluster):
    """Test that failed_files_ttl_sec cleanup does NOT reset the retry counter.

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
    # - failed_files_ttl_sec=2: TTL cleanup tries to delete nodes after 2 seconds
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
            "failed_files_ttl_sec": 2,  # TTL shorter than retry interval
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
                # Capture the confirming observation. The terminal failed node is
                # itself subject to failed_files_ttl_sec (2s here), so re-reading
                # Keeper after the loop can legitimately find it already swept.
                observed_retries = retries
                observed_terminal = is_terminal
                observed_status = status
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

    # Final verification, against what was observed at the moment the file reached
    # the terminal state. Deleting terminal failed nodes after failed_files_ttl_sec
    # is the feature under test, so Keeper must not be re-read here.
    assert observed_terminal, \
        f"File should have reached terminal failed state (no .retriable suffix), status: {observed_status}"
    assert observed_retries == 3, \
        f"Terminal failed node should have retries=3, got: {observed_retries}"

    logging.info("Test passed: retry counter was NOT reset by TTL cleanup")

    # Cleanup
    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_drop_failed_files_privilege(started_cluster):
    """`SYSTEM DROP S3QUEUE FAILED FILES` must be gated on the table-scoped
    `SYSTEM_DROP_S3QUEUE_FAILED_FILES` privilege.

    Covers both entry points that check it:
      - `InterpreterSystemQuery::dropObjectStorageQueueFailedFiles` -> `context->checkAccess(...)`
      - `getRequiredAccessForDDLOnCluster()` for the `ON CLUSTER` form

    Both branches of each entry point are asserted: denied without the privilege, allowed
    with it. The `ON CLUSTER` allow branch is the one worth stating explicitly - a
    `getRequiredAccessForDDLOnCluster()` that asked for the wrong access type or the wrong
    scope would refuse a correctly-privileged user forever, and a deny-only test would
    still pass.

    `executeDDLQueryOnCluster` resolves the cluster before checking access, so the cluster
    has to exist for a denial to be attributable to the privilege rather than to an unknown
    cluster. The table, the user and its grants therefore exist on both replicas of
    `cluster`; the user and grants are created `ON CLUSTER` so they reach both.

    The user is also granted `CLUSTER`, which every ON CLUSTER statement needs on its own. Without
    it the ON CLUSTER denial below is produced by the missing `CLUSTER` grant rather than by this
    feature's gate, and the ON CLUSTER allow branch is unreachable.

    Access control only. Whether the drop does the right thing to `/failed` on either
    replica is `test_drop_failed_files_on_cluster_concurrent`'s subject, not this one's.
    """
    node = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_drop_priv_{uuid.uuid4().hex[:8]}"
    user_name = f"user_drop_priv_{uuid.uuid4().hex[:8]}"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    # On both replicas, sharing one `keeper_path`: the ON CLUSTER form runs on every
    # replica of `cluster`, and on one that does not have the table it would fail with
    # UNKNOWN_TABLE - which says nothing about access control.
    for replica in (node, node2):
        create_table(
            started_cluster,
            replica,
            table_name,
            "unordered",
            files_path,
            additional_settings={"keeper_path": keeper_path},
        )

    # `CREATE USER` and `GRANT` are local to the replica that runs them, so both are run
    # ON CLUSTER: the ON CLUSTER drop is checked against this user on every replica, and
    # a user known only to `instance` would be denied on `instance2` for the wrong reason.
    node.query(
        f"CREATE USER {user_name} ON CLUSTER cluster IDENTIFIED WITH no_password"
    )
    # The command resolves the table, so the user must be able to see it at all;
    # otherwise a failure could be UNKNOWN_TABLE rather than a privilege denial.
    node.query(
        f"GRANT SHOW TABLES ON default.{table_name} TO {user_name} ON CLUSTER cluster"
    )
    # `access_control_improvements.on_cluster_queries_require_cluster_grant` defaults to true, so
    # running *any* ON CLUSTER statement needs `CLUSTER` on top of whatever the statement itself
    # requires. Granted here, before the denial assertions, so that the only privilege still missing
    # below is the S3Queue one - otherwise the ON CLUSTER denial says nothing about this feature's
    # gate, and the ON CLUSTER allow branch cannot be reached at all.
    node.query(f"GRANT CLUSTER ON *.* TO {user_name} ON CLUSTER cluster")

    direct_query = f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}"
    on_cluster_query = (
        f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name} ON CLUSTER cluster"
    )

    # The denials below are matched on the name of the missing grant, not just on `ACCESS_DENIED`.
    # Any missing privilege produces `ACCESS_DENIED`, so the weaker check passes whether or not this
    # feature has a gate of its own - it did exactly that here while `CLUSTER` was the privilege
    # actually missing. Naming the grant is what ties the denial to this statement.
    required_grant = f"SYSTEM DROP S3QUEUE FAILED FILES ON default.{table_name}"

    # 1. Denied without the privilege, direct form.
    direct_error = node.query_and_get_error(direct_query, user=user_name)
    assert "ACCESS_DENIED" in direct_error, direct_error
    assert required_grant in direct_error, direct_error

    # 2. Denied without the privilege, ON CLUSTER form. This exercises
    #    getRequiredAccessForDDLOnCluster(), a separate path from the
    #    checkAccess() call inside the interpreter.
    on_cluster_error = node.query_and_get_error(on_cluster_query, user=user_name)
    assert "ACCESS_DENIED" in on_cluster_error, on_cluster_error
    assert required_grant in on_cluster_error, on_cluster_error

    # 3. Grant exactly the new privilege, table-scoped.
    node.query(
        f"GRANT SYSTEM DROP S3QUEUE FAILED FILES ON default.{table_name} "
        f"TO {user_name} ON CLUSTER cluster"
    )

    # 4. The direct form now succeeds. query() raises on any error, so reaching the
    #    next statement is itself the assertion.
    node.query(direct_query, user=user_name)

    # 5. The ON CLUSTER form now succeeds too. This is what the denial in step 2 cannot
    #    establish: `getRequiredAccessForDDLOnCluster()` must not only refuse the
    #    unprivileged user, it must also admit the privileged one. Same assertion
    #    mechanism as step 4 - reaching the next statement means no error was raised.
    node.query(on_cluster_query, user=user_name)

    # 6. The privilege is table-scoped and must not leak to a different table.
    other_table_name = f"test_drop_priv_other_{uuid.uuid4().hex[:8]}"
    create_table(
        started_cluster,
        node,
        other_table_name,
        "unordered",
        f"{other_table_name}_data",
        additional_settings={"keeper_path": f"/clickhouse/test_{other_table_name}"},
    )
    node.query(f"GRANT SHOW TABLES ON default.{other_table_name} TO {user_name}")
    other_error = node.query_and_get_error(
        f"SYSTEM DROP S3QUEUE FAILED FILES default.{other_table_name}", user=user_name
    )
    assert "ACCESS_DENIED" in other_error, other_error
    assert (
        f"SYSTEM DROP S3QUEUE FAILED FILES ON default.{other_table_name}" in other_error
    ), other_error

    # Cleanup. The user and `table_name` exist on both replicas, so they are dropped
    # ON CLUSTER too; `other_table_name` was only ever created on `node`.
    node.query(f"DROP USER {user_name} ON CLUSTER cluster")
    node.query(f"DROP TABLE {table_name} ON CLUSTER cluster")
    node.query(f"DROP TABLE {other_table_name}")


def test_drop_failed_files_on_cluster_concurrent(started_cluster):
    """`SYSTEM DROP S3QUEUE FAILED FILES ... ON CLUSTER` must be idempotent when
    several replicas execute it at the same time, and must leave no stale state
    behind on the replica that loses the `cleanup_lock` race.

    Guards the ON CLUSTER concurrent-drop path raised in review on #113784: only
    one replica wins `cleanup_lock` and performs the Keeper deletes, while every
    other replica takes the loser path in `waitForConcurrentDropToComplete`,
    waits for the winner, verifies `/failed` is empty and must then also
    reconcile its own in-memory `local_file_statuses`. A regression shows up
    either as a spurious exception on the loser, or as `Failed` cache entries
    surviving on a replica that did not do the deleting.

    On the contract being asserted: the loser is expected to *succeed*, not to
    raise. The command was made idempotent for ON CLUSTER execution precisely so
    that a concurrent invocation is not an error, so an exception from either
    call is a failure of this test rather than an accepted outcome.
    """
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_drop_on_cluster_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    num_failing_files = 5

    # The same keeper_path on both replicas is how S3Queue replicates.
    for node in (node1, node2):
        create_table(
            started_cluster,
            node,
            table_name,
            "unordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_loading_retries": 0,  # fail terminally on the first attempt
                # Keep the periodic sweep out of the way, so the only thing that
                # removes /failed nodes is the explicit ON CLUSTER command.
                "failed_files_ttl_sec": 0,
                "tracked_files_limit": 0,
            },
        )

    # Files that cannot be parsed against the table's schema -> terminal failures.
    invalid_csv = b"not,valid,numbers\n"
    for i in range(num_failing_files):
        put_s3_file_content(started_cluster, f"{files_path}/failed_{i}.csv", invalid_csv)

    # Both replicas must be active consumers: a replica only registers itself in
    # `<keeper_path>/registry` while it has attached views (see `registerActive` in
    # StorageObjectStorageQueue), and only registered replicas take part in the hash
    # ring. With a view on one replica only, the other never becomes a participant
    # and never populates its own cache, so the post-drop cache assertion below
    # would be vacuous for it.
    for node in (node1, node2):
        create_mv(node, table_name, dst_table_name)

    def failed_znodes():
        result = node1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    def cached_failed(node):
        return int(
            node.query(
                f"SELECT count() FROM system.s3queue_metadata_cache "
                f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
            ).strip()
        )

    def wait_for(predicate, timeout_sec=120):
        """Poll for a state instead of sleeping on a fixed schedule."""
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.5)
        return False

    # All files must have failed, and their /failed znodes must exist, before dropping.
    assert wait_for(
        lambda: failed_znodes() >= num_failing_files
    ), f"expected {num_failing_files} failed znodes, got {failed_znodes()}"

    # The failures must be visible in the replicas' caches before dropping,
    # otherwise "cache is empty afterwards" would prove nothing.
    #
    # Why this sums the two caches instead of requiring each replica to reach
    # `num_failing_files` on its own: `create_table` turns on
    # `enable_hash_ring_filtering` by default, so in Unordered mode
    # `filterOutForProcessor` hands each file to exactly one replica, and only the
    # replica that claimed a file records `Failed` for it. No replica ever sees all
    # of them, so a per-replica `>= num_failing_files` precondition cannot hold at
    # all. Disabling the hash ring does not fix that either: the unconditional
    # already-processed/already-failed filter runs before a file is ever claimed and
    # never touches the cache, so the second replica still records nothing for files
    # the first one failed.
    #
    # Why it does not even require "at least one cached on each replica": the ring is
    # a *consistent* hash ring, so it gives no lower bound on any single replica's
    # share. With 5 files over 2 replicas an all-to-one split (5/0 or 0/5) has
    # probability 2 * 0.5**5 = 1/16, about 6.25%, and that is a floor rather than an
    # estimate - a replica whose registry view is still empty processes everything,
    # which biases the split further toward lopsided. So "at least one each" would be
    # a test that fails roughly one run in sixteen.
    #
    # The weakness this accepts in exchange: the summed form never fails spuriously,
    # but in the ~3% of runs where the split is all-to-one *and* the replica holding
    # the entries is the one that wins the `cleanup_lock` race, the loser's
    # `reconcileFailedFilesCache` has nothing to remove and the post-drop assertions
    # below pass without proving anything. That gap is closed deterministically by
    # `test_drop_failed_files_loser_reconciles_cache`, not by tightening this
    # precondition - please do not "fix" it back into a flaky one.
    #
    # What must hold here is that every failure is cached by whichever replica owned
    # the file, i.e. the two caches together account for all of them.
    assert wait_for(
        lambda: cached_failed(node1) + cached_failed(node2) >= num_failing_files
    ), (
        f"caches not populated: instance={cached_failed(node1)}, "
        f"instance2={cached_failed(node2)}, failed znodes={failed_znodes()}"
    )
    logging.debug(
        "Failed entries cached before drop: instance=%s, instance2=%s",
        cached_failed(node1),
        cached_failed(node2),
    )

    # Both replicas issue the ON CLUSTER drop at the same time. The barrier makes
    # the overlap deterministic without depending on sleep timing.
    query = f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name} ON CLUSTER cluster"
    barrier = threading.Barrier(2)

    def run(node):
        # Bounded: if the other thread never arrives, the barrier breaks and this
        # raises instead of blocking the pool's shutdown forever.
        barrier.wait(timeout=60)
        return node.query(query)

    errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(run, node1): "instance",
            pool.submit(run, node2): "instance2",
        }
        for future, origin in futures.items():
            try:
                future.result(timeout=180)
            except Exception as e:
                errors.append(f"{origin}: {e}")

    # Neither invocation may raise: concurrent ON CLUSTER drops are idempotent.
    assert not errors, f"concurrent ON CLUSTER drop raised: {errors}"

    # No failed znodes left in Keeper.
    assert wait_for(
        lambda: failed_znodes() == 0
    ), f"failed znodes remain after drop: {failed_znodes()}"

    # And no stale Failed entries in either replica's in-memory cache, including
    # on whichever replica lost the cleanup_lock race.
    assert wait_for(
        lambda: cached_failed(node1) == 0
    ), f"instance still caches Failed entries: {cached_failed(node1)}"
    assert wait_for(
        lambda: cached_failed(node2) == 0
    ), f"instance2 still caches Failed entries: {cached_failed(node2)}"

    # Cleanup
    for node in (node1, node2):
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        node.query(f"DROP TABLE IF EXISTS {dst_table_name}")


PAUSE_BEFORE_CLEANUP_LOCK_READ_FAILPOINT = "object_storage_queue_pause_before_cleanup_lock_read"


def _wait_failpoint_paused(node, failpoint, timeout=120):
    """Block until a thread on `node` parks at `failpoint`.

    `SYSTEM WAIT FAILPOINT ... PAUSE` blocks, so it has to run on a worker thread: a failpoint that is
    never reached must fail the test rather than hang it. The executor is deliberately not joined on
    the failure path, because its worker is still stuck inside the blocking query.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(node.query, f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")
    done, _ = concurrent.futures.wait([future], timeout=timeout)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        raise AssertionError(f"failpoint {failpoint} was not reached within {timeout}s")
    pool.shutdown(wait=False)
    future.result()


def test_drop_failed_files_survives_lock_vanishing_before_it_is_read(started_cluster):
    """A replica that loses `cleanup_lock` and then finds it already released must not fail the command.

    The `tryCreate` -> `get` window: a replica loses the lock race, and by the time it reads the lock to
    see whose it was, the holder has finished and released it. Its `Stat` was never read, so there is no
    attempt to bind to and no command id to match a published result against.

    That case used to fall back to "is `/failed` globally empty" as its only observation, which is a
    stronger postcondition than any winner enforces - a winner deletes the snapshot it opened with and is
    not answerable for files that fail afterwards. So a single new failure landing after the winner's
    snapshot made this replica throw `KEEPER_EXCEPTION` about a cleanup that had actually succeeded.

    The window is microseconds wide in production, so it is opened with a failpoint rather than raced
    for. Two replicas share the queue, `instance2` is parked between its failed `tryCreate` and the
    `get`, the winner is played by hand so that it publishes and releases while `instance2` is parked,
    and a new file then fails. On resume, `instance2` must take the lock itself and drop what is there.

    The drop is issued directly on `instance2` rather than `ON CLUSTER`, even though `ON CLUSTER` is how
    the problem was originally reported. `DDLWorker` classifies `KEEPER_EXCEPTION` as retriable and
    re-runs the task a few seconds later, by which time the lock is free and the retry succeeds - so the
    `ON CLUSTER` form hides this bug from the client behind a delay and an error in the log, and a test
    written on it passes either way. The direct form runs the same `waitForConcurrentDropToComplete`
    path with nothing to paper over the result.
    """
    node1 = started_cluster.instances["instance"]
    node2 = started_cluster.instances["instance2"]

    table_name = f"test_drop_lock_vanished_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    cleanup_lock_path = f"{keeper_path}/cleanup_lock"
    num_failing_files = 3

    for node in (node1, node2):
        create_table(
            started_cluster,
            node,
            table_name,
            "unordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "s3queue_loading_retries": 0,  # fail terminally on the first attempt
                # Nothing but this test and the command under test may touch /failed.
                "failed_files_ttl_sec": 0,
                "tracked_files_limit": 0,
            },
        )

    invalid_csv = b"not,valid,numbers\n"
    for i in range(num_failing_files):
        put_s3_file_content(started_cluster, f"{files_path}/failed_{i}.csv", invalid_csv)

    for node in (node1, node2):
        create_mv(node, table_name, dst_table_name)

    def failed_znodes():
        result = node1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    def wait_for(predicate, timeout_sec=120):
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.5)
        return False

    assert wait_for(
        lambda: failed_znodes() >= num_failing_files
    ), f"expected {num_failing_files} failed znodes, got {failed_znodes()}"

    # Stop consuming before touching Keeper by hand, so nothing re-fails files behind the test's back.
    for node in (node1, node2):
        node.query(f"DROP TABLE {mv_name}")

    zk = started_cluster.get_kazoo_client("zoo1")

    def take_cleanup_lock():
        try:
            zk.create(cleanup_lock_path, TEST_DROP_LOCK_VALUE, ephemeral=True)
            return True
        except NodeExistsError:
            return False

    assert wait_for(
        take_cleanup_lock, timeout_sec=60
    ), "could not acquire cleanup_lock for the test"

    # Park instance2 between its failed `tryCreate` and the `get`. Armed before the command starts, so
    # there is no race over which of the two gets there first.
    node2.query(f"SYSTEM ENABLE FAILPOINT {PAUSE_BEFORE_CLEANUP_LOCK_READ_FAILPOINT}")

    drop_result = {}

    def run_drop():
        try:
            node2.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")
        except Exception as e:  # noqa: BLE001 - reported through the assertion below
            drop_result["error"] = e

    drop_thread = threading.Thread(target=run_drop)
    drop_thread.start()
    try:
        _wait_failpoint_paused(node2, PAUSE_BEFORE_CLEANUP_LOCK_READ_FAILPOINT)

        # Play the winner: delete its whole snapshot, publish the verdict it publishes before releasing,
        # then release the lock. instance1 polls every 100ms and accepts this result by command id.
        snapshot = zk.get_children(failed_path)
        for child in snapshot:
            zk.delete(f"{failed_path}/{child}")
        zk.create(
            f"{keeper_path}/last_drop_result",
            json.dumps(
                {
                    "command_id": TEST_DROP_COMMAND_ID,
                    "attempt_id": str(zk.exists(cleanup_lock_path).czxid),
                    "success": True,
                    "snapshot_size": len(snapshot),
                    "deleted": len(snapshot),
                    "error": "",
                }
            ).encode(),
        )
        zk.delete(cleanup_lock_path)

        # A file fails after the winner's snapshot. It is not part of the completed attempt, and it is
        # what used to make the parked replica reject a cleanup that had succeeded.
        zk.create(
            f"{failed_path}/failed_after_the_winner_released",
            json.dumps(
                {
                    "file_path": "failed_after_the_winner_released.csv",
                    "last_processed_timestamp": 0,
                    "last_exception": "failed after the winner released the lock",
                    "retries": 0,
                    "processor_id": "",
                }
            ).encode(),
        )

        # Resume instance2. Its `get` now raises ZNONODE: the lock it lost is already gone.
        node2.query(f"SYSTEM DISABLE FAILPOINT {PAUSE_BEFORE_CLEANUP_LOCK_READ_FAILPOINT}")
    finally:
        drop_thread.join(timeout=300)

    assert not drop_thread.is_alive(), "drop did not return"
    assert "error" not in drop_result, (
        f"the drop failed on a replica whose lock vanished before it could be read: "
        f"{drop_result.get('error')}"
    )

    # The bug's own signature, checked separately: even where something upstream retries the statement
    # and hides the failure from the client, this line in the log means the replica rejected a cleanup
    # it could not name rather than redoing it.
    assert not node2.contains_in_log(
        "so the cleanup cannot be confirmed"
    ), "the replica rejected the cleanup instead of retrying it"

    # instance2 could not verify anything, so it had to take the lock and do the work itself - which
    # means the file that failed after the winner's snapshot is gone too.
    assert wait_for(
        lambda: failed_znodes() == 0
    ), f"failed znodes remain after the drop: {failed_znodes()}"

    for node in (node1, node2):
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        node.query(f"DROP TABLE IF EXISTS {dst_table_name}")


def test_drop_failed_files_loser_reconciles_cache(started_cluster):
    """The replica that loses the `cleanup_lock` race must still reconcile its own
    `local_file_statuses`, so no stale `Failed` entries survive in
    `system.s3queue_metadata_cache`.

    `test_drop_failed_files_on_cluster_concurrent` covers the realistic ON CLUSTER
    shape, but it cannot guarantee that the replica which loses the race is the one
    holding cached entries - and reconciling an empty cache is a silent no-op, so
    that test can pass without ever proving this path. This test removes the race
    entirely and drives the loser branch directly:

      1. one replica fails every file, so its cache definitely holds the entries;
      2. its materialized view is dropped, so nothing can re-fail the files and
         repopulate `/failed` behind our back;
      3. the test itself takes `<keeper_path>/cleanup_lock` with the winner's own
         marker value `manual_drop_failed:<command_id>`, so `dropFailedFiles` finds the lock held
         and takes the loser branch (`EphemeralNodeHolder::tryCreate` fails ->
         `waitForConcurrentDropToComplete`);
      4. the test then plays the winner by hand: it deletes the `/failed` children
         and only then releases the lock, which is the order the protocol expects -
         releasing first would make the loser's verification find terminal nodes
         still present and raise `KEEPER_EXCEPTION`;
      5. the loser must return normally, and its cache must come back empty.

    The final assertion is the point of the test. With `failed_files_ttl_sec = 0` and
    `tracked_files_limit = 0` the periodic sweep never touches `/failed` and never
    calls `reconcileFailedFilesCache` itself, and the view is gone so nothing else
    writes to the cache. An empty cache is therefore attributable to exactly one
    thing: the loser's own `verifyCleanupSucceeded` -> `reconcileFailedFilesCache` ->
    `removeStaleFailedCacheEntries` having actually run and removed all five entries.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_drop_failed_loser_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    cleanup_lock_path = f"{keeper_path}/cleanup_lock"
    num_failing_files = 5

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,  # fail terminally on the first attempt
            # Keep the periodic sweep away from /failed, so the only thing that can
            # empty the cache is the drop command's own reconciliation.
            "failed_files_ttl_sec": 0,
            "tracked_files_limit": 0,
        },
    )

    invalid_csv = b"not,valid,numbers\n"
    for i in range(num_failing_files):
        put_s3_file_content(started_cluster, f"{files_path}/failed_{i}.csv", invalid_csv)

    create_mv(node, table_name, dst_table_name)

    def failed_znodes():
        result = node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    def cached_failed():
        return int(
            node.query(
                f"SELECT count() FROM system.s3queue_metadata_cache "
                f"WHERE zookeeper_path = '{keeper_path}' AND status = 'Failed'"
            ).strip()
        )

    def wait_for(predicate, timeout_sec=120):
        """Poll for a state instead of sleeping on a fixed schedule."""
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.5)
        return False

    assert wait_for(
        lambda: failed_znodes() >= num_failing_files
    ), f"expected {num_failing_files} failed znodes, got {failed_znodes()}"
    assert wait_for(
        lambda: cached_failed() >= num_failing_files
    ), f"cache not populated: {cached_failed()}"

    # Stop consuming before touching Keeper. Once the /failed nodes are deleted the
    # files become eligible again, and a re-failure landing between the deletion and
    # the loser's verification would make it find terminal nodes and raise. Dropping
    # the view takes `dependencies_count` to zero so `threadFunc` stops entering
    # `streamToViews` at all; it does not clear `local_file_statuses`, so the five
    # cached Failed entries stay exactly where they are.
    node.query(f"DROP TABLE {mv_name}")

    zk = started_cluster.get_kazoo_client("zoo1")

    # Hold the lock with the winner's marker value. The background cleanup sweep
    # briefly takes and releases this same lock, so retry until it is ours.
    def take_cleanup_lock():
        try:
            zk.create(cleanup_lock_path, TEST_DROP_LOCK_VALUE, ephemeral=True)
            return True
        except NodeExistsError:
            return False

    assert wait_for(
        take_cleanup_lock, timeout_sec=60
    ), "could not acquire cleanup_lock for the test"

    drop_result = {}

    def run_drop():
        try:
            node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")
        except Exception as e:  # noqa: BLE001 - reported through the assertion below
            drop_result["error"] = e

    drop_thread = threading.Thread(target=run_drop)
    drop_thread.start()
    try:
        # Wait until the command has actually taken the loser branch. Releasing the
        # lock before this point would let it win the lock and do the deleting
        # itself, which is the path this test is not about.
        waiting_message = (
            f"{keeper_path}): Another replica is executing "
            f"SYSTEM DROP S3QUEUE FAILED FILES"
        )
        assert wait_for(
            lambda: node.contains_in_log(waiting_message)
        ), "drop command did not reach the loser branch"

        # Play the winner: delete the terminal /failed nodes first, release the lock
        # second. The loser polls the lock every 100ms and verifies /failed on the
        # first poll that finds it gone.
        for child in zk.get_children(failed_path):
            zk.delete(f"{failed_path}/{child}")
        logging.debug("Deleted %s /failed children as the winner would", num_failing_files)
        zk.delete(cleanup_lock_path)
    finally:
        drop_thread.join(timeout=300)

    assert not drop_thread.is_alive(), "drop command did not return after the lock was released"
    # The loser is expected to succeed: the command is idempotent for concurrent
    # execution, so an exception here is a failure rather than an accepted outcome.
    assert "error" not in drop_result, f"loser replica raised: {drop_result.get('error')}"

    assert failed_znodes() == 0, f"failed znodes remain: {failed_znodes()}"
    # The assertion this test exists for: the loser reconciled its own cache.
    assert cached_failed() == 0, f"loser still caches Failed entries: {cached_failed()}"

    # Cleanup
    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(f"DROP TABLE IF EXISTS {dst_table_name}")


def _drive_loser_branch(
    started_cluster, table_name, num_failing_files, play_winner, release_lock=True
):
    """Put a `SYSTEM DROP S3QUEUE FAILED FILES` on the loser branch and let the caller be the winner.

    Same construction as `test_drop_failed_files_loser_reconciles_cache`: the race is removed
    entirely rather than raced for, by taking `<keeper_path>/cleanup_lock` with the winner's own
    marker value before running the command, so `EphemeralNodeHolder::tryCreate` fails and
    `dropFailedFiles` takes `waitForConcurrentDropToComplete`.

    `play_winner(zk, keeper_path, failed_path, failed_children)` is called while the lock is still
    held and does whatever the winner under test would have done. The lock is released afterwards,
    unless `release_lock` is false - which a caller that hands the lock to a *different* attempt
    inside `play_winner` wants, so that the later attempt is still holding it when the command
    under test makes up its mind.

    Returns the exception the command raised, or `None` if it returned normally.
    """
    node = started_cluster.instances["instance"]

    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    cleanup_lock_path = f"{keeper_path}/cleanup_lock"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,  # fail terminally on the first attempt
            # Keep the periodic sweep away from /failed, so nothing but the drop command
            # and this test touches it.
            "failed_files_ttl_sec": 0,
            "tracked_files_limit": 0,
        },
    )

    invalid_csv = b"not,valid,numbers\n"
    for i in range(num_failing_files):
        put_s3_file_content(started_cluster, f"{files_path}/failed_{i}.csv", invalid_csv)

    create_mv(node, table_name, dst_table_name)

    def failed_znodes():
        result = node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    def wait_for(predicate, timeout_sec=120):
        """Poll for a state instead of sleeping on a fixed schedule."""
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.5)
        return False

    assert wait_for(
        lambda: failed_znodes() >= num_failing_files
    ), f"expected {num_failing_files} failed znodes, got {failed_znodes()}"

    # Stop consuming before touching Keeper, so nothing re-fails files behind the test's back.
    node.query(f"DROP TABLE {mv_name}")

    zk = started_cluster.get_kazoo_client("zoo1")

    def take_cleanup_lock():
        try:
            zk.create(cleanup_lock_path, TEST_DROP_LOCK_VALUE, ephemeral=True)
            return True
        except NodeExistsError:
            return False

    assert wait_for(
        take_cleanup_lock, timeout_sec=60
    ), "could not acquire cleanup_lock for the test"

    drop_result = {}

    def run_drop():
        try:
            node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")
        except Exception as e:  # noqa: BLE001 - reported through the caller's assertions
            drop_result["error"] = e

    drop_thread = threading.Thread(target=run_drop)
    drop_thread.start()
    try:
        waiting_message = (
            f"{keeper_path}): Another replica is executing "
            f"SYSTEM DROP S3QUEUE FAILED FILES"
        )
        assert wait_for(
            lambda: node.contains_in_log(waiting_message)
        ), "drop command did not reach the loser branch"

        play_winner(zk, keeper_path, failed_path, zk.get_children(failed_path))
        if release_lock:
            zk.delete(cleanup_lock_path)
    finally:
        drop_thread.join(timeout=300)

    assert not drop_thread.is_alive(), "drop command did not return once its attempt had finished"

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(f"DROP TABLE IF EXISTS {dst_table_name}")

    return drop_result.get("error")


def test_drop_failed_files_loser_succeeds_when_new_failures_arrive_during_drop(started_cluster):
    """A file failing while the winner works must not make the loser reject the winner's cleanup.

    The winner deletes the snapshot of terminal nodes it took when it started, and that is all it
    is responsible for. The loser used to demand that `/failed` be globally empty instead, so any
    file failing between the winner's snapshot and the loser's verification made the winner report
    success while every loser raised `KEEPER_EXCEPTION` - and on a queue that is actively failing
    files that is the ordinary interleaving, not a rare one.

    Here the winner is played by hand: it deletes its whole snapshot, publishes the success result
    the real winner publishes before releasing the lock, and only then a new file fails. The loser
    must accept the winner's own verdict and return normally even though `/failed` is not empty.
    """
    num_failing_files = 5
    table_name = f"test_drop_new_failures_{uuid.uuid4().hex[:8]}"

    def play_winner(zk, keeper_path, failed_path, failed_children):
        # The winner's snapshot, deleted in full.
        for child in failed_children:
            zk.delete(f"{failed_path}/{child}")

        # The winner publishes what it did, while it still holds the lock. `attempt_id` is the czxid of
        # the lock node it holds, which is what binds the result to this attempt.
        zk.create(
            f"{keeper_path}/last_drop_result",
            json.dumps(
                {
                    "command_id": TEST_DROP_COMMAND_ID,
                    "attempt_id": str(zk.exists(f"{keeper_path}/cleanup_lock").czxid),
                    "success": True,
                    "snapshot_size": len(failed_children),
                    "deleted": len(failed_children),
                    "error": "",
                }
            ).encode(),
        )

        # A file fails after the winner's snapshot was taken. It is not part of this attempt.
        zk.create(
            f"{failed_path}/failed_after_the_snapshot",
            json.dumps(
                {
                    "file_path": "failed_after_the_snapshot.csv",
                    "last_processed_timestamp": 0,
                    "last_exception": "failed while the drop was running",
                    "retries": 0,
                    "processor_id": "",
                }
            ).encode(),
        )

    error = _drive_loser_branch(
        started_cluster, table_name, num_failing_files, play_winner
    )

    assert error is None, f"loser rejected a successful cleanup: {error}"


def test_drop_failed_files_reports_partial_failure_to_losers(started_cluster):
    """A winner that only partly succeeded must say so, and the loser must report the winner's verdict.

    This is the case the emptiness check was originally there to catch, and it still has to work:
    "lock released" alone never meant "cleanup succeeded". The difference is where the answer comes
    from - the winner's published result rather than the loser's guess at global state - so the
    message carries the winner's own numbers instead of a count of whatever happens to remain.
    """
    num_failing_files = 5
    table_name = f"test_drop_partial_{uuid.uuid4().hex[:8]}"

    def play_winner(zk, keeper_path, failed_path, failed_children):
        # The winner got through part of its snapshot and then hit a Keeper error.
        deleted = 2
        for child in failed_children[:deleted]:
            zk.delete(f"{failed_path}/{child}")

        zk.create(
            f"{keeper_path}/last_drop_result",
            json.dumps(
                {
                    "command_id": TEST_DROP_COMMAND_ID,
                    "attempt_id": str(zk.exists(f"{keeper_path}/cleanup_lock").czxid),
                    "success": False,
                    "snapshot_size": len(failed_children),
                    "deleted": deleted,
                    "error": "Failed to remove 1 batch(es) of failed file nodes",
                }
            ).encode(),
        )

    error = _drive_loser_branch(
        started_cluster, table_name, num_failing_files, play_winner
    )

    assert error is not None, "loser accepted a cleanup the winner reported as failed"
    message = str(error)
    assert "did not complete" in message, message
    # The winner's own numbers, not a count of what remains in /failed.
    assert "dropped 2 of the 5" in message, message
    assert "Failed to remove 1 batch(es)" in message, message


def _hand_the_lock_to_a_later_attempt(zk, keeper_path, marker_payload=None):
    """Atomically release this command's lock and give a *different* command the same path.

    The replacement lock carries `OTHER_DROP_COMMAND_ID`, not this command's id. That is what makes
    these tests about an unrelated operation taking the path: were the id the same, the waiter would
    correctly read it as its own command retrying and keep waiting.

    The interleaving under test is "the lock is released and re-acquired inside one poll interval",
    which looks timing-bound but need not be: a Keeper multi transaction makes the gap unobservable
    by construction, so the waiter cannot see the path empty no matter when it polls. That is what
    makes this test deterministic rather than a race against the 100ms poll.

    `marker_payload`, when given, is written in the same transaction, so the later attempt's result
    is already in place the first time the waiter looks.
    """
    cleanup_lock_path = f"{keeper_path}/cleanup_lock"

    transaction = zk.transaction()
    transaction.delete(cleanup_lock_path)
    transaction.create(cleanup_lock_path, OTHER_DROP_LOCK_VALUE, ephemeral=True)
    if marker_payload is not None:
        transaction.set_data(
            f"{keeper_path}/last_drop_result", json.dumps(marker_payload).encode()
        )
    results = transaction.commit()
    assert all(
        not isinstance(r, Exception) for r in results
    ), f"the lock handoff transaction did not commit: {results}"


def test_drop_failed_files_waiter_is_not_transferred_to_a_later_attempt(started_cluster):
    """A waiter must decide on the attempt it started waiting for, not on whoever holds the lock next.

    `cleanup_lock` is a fixed path, so every attempt creates and deletes the same node. The waiter
    polled it for existence only, which cannot distinguish "the attempt I am watching still holds
    the lock" from "that attempt finished and an unrelated one took the path". A release and a
    re-acquisition inside one 100ms poll interval was therefore invisible, and the waiter silently
    carried on watching an attempt that had started after its own command did.

    Here attempt A completes and publishes, and the lock passes to attempt B in a single Keeper
    transaction, so the waiter provably never observes the path empty. B then keeps the lock. The
    waiter must still return on A's result rather than blocking on B, which it has no business
    waiting for.
    """
    num_failing_files = 5
    table_name = f"test_drop_handoff_{uuid.uuid4().hex[:8]}"

    def play_winner(zk, keeper_path, failed_path, failed_children):
        # A does its job in full and publishes its own verdict.
        for child in failed_children:
            zk.delete(f"{failed_path}/{child}")
        zk.create(
            f"{keeper_path}/last_drop_result",
            json.dumps(
                {
                    "command_id": TEST_DROP_COMMAND_ID,
                    "attempt_id": str(zk.exists(f"{keeper_path}/cleanup_lock").czxid),
                    "success": True,
                    "snapshot_size": len(failed_children),
                    "deleted": len(failed_children),
                    "error": "",
                }
            ).encode(),
        )

        # A releases and B acquires, atomically. B then holds the lock and does nothing.
        _hand_the_lock_to_a_later_attempt(zk, keeper_path)

    # `release_lock=False`: B is deliberately left holding the lock, so a waiter which followed the
    # path instead of the attempt would still be blocked when the command is expected to have returned.
    error = _drive_loser_branch(
        started_cluster, table_name, num_failing_files, play_winner, release_lock=False
    )

    assert error is None, f"waiter did not accept the result of the attempt it waited for: {error}"


def test_drop_failed_files_waiter_does_not_adopt_a_later_attempts_verdict(started_cluster):
    """A later attempt's published result must not be reported as the awaited attempt's outcome.

    The marker was accepted on the strength of its Keeper version being higher than the one read
    before waiting. That excludes a stale result from an earlier attempt, which was the intent, but
    it does not exclude a *newer* result from an unrelated later one - so a waiter could be handed
    the verdict of a drop it never waited for, in either direction.

    Attempt A succeeds and empties `/failed`; the lock then passes to B, which publishes a failure
    of its own, all in one transaction so the waiter's first look already sees B's marker. The
    waiter must not report B's failure. It cannot report A's either - the marker is a single node
    kept in place, so B's write destroyed it - and must fall back to what it can still observe,
    which is that `/failed` is empty.
    """
    num_failing_files = 5
    table_name = f"test_drop_foreign_verdict_{uuid.uuid4().hex[:8]}"
    b_error_text = "a totally unrelated later attempt failed"

    def play_winner(zk, keeper_path, failed_path, failed_children):
        # A empties /failed and publishes success.
        for child in failed_children:
            zk.delete(f"{failed_path}/{child}")
        zk.create(
            f"{keeper_path}/last_drop_result",
            json.dumps(
                {
                    "command_id": TEST_DROP_COMMAND_ID,
                    "attempt_id": str(zk.exists(f"{keeper_path}/cleanup_lock").czxid),
                    "success": True,
                    "snapshot_size": len(failed_children),
                    "deleted": len(failed_children),
                    "error": "",
                }
            ).encode(),
        )

        # The lock passes to B and B's failure overwrites A's result, in one transaction. `attempt_id`
        # is deliberately a value no lock node can have, standing in for B's own id: what matters is
        # only that it is not the id the waiter is bound to.
        _hand_the_lock_to_a_later_attempt(
            zk,
            keeper_path,
            marker_payload={
                # The other command's own verdict - which is the point: it must not be adopted.
                "command_id": OTHER_DROP_COMMAND_ID,
                "attempt_id": "-1",
                "success": False,
                "snapshot_size": 99,
                "deleted": 0,
                "error": b_error_text,
            },
        )

    error = _drive_loser_branch(
        started_cluster, table_name, num_failing_files, play_winner, release_lock=False
    )

    assert error is None, f"waiter adopted a later attempt's verdict or failed to fall back: {error}"


def test_drop_failed_files_waiter_follows_its_command_across_a_retry(started_cluster):
    """A waiter must keep waiting when the command it waits for retries, not give up on it.

    `dropFailedFiles` retries the whole statement when an attempt loses its Keeper session, and each
    attempt takes the lock again, which gives the lock node a new `czxid`. A waiter bound to that
    `czxid` read the retry as "a different operation took the path", stopped waiting and evaluated at
    once - at the moment the retry had just acquired the lock and deleted nothing, so `/failed` was at
    its fullest - found no result under the id it was bound to, and threw. The retry then finished and
    reported success: the winner succeeded while every waiter failed.

    Binding to the command id instead makes a retry recognisable as the same statement. This test
    drives exactly that sequence: the lock is released and retaken under the *same* command id while
    `/failed` is still full, the waiter must not treat that as the end of the command, and it must
    then accept the verdict the retry eventually publishes.

    The `is_alive` window in the middle is the regression assertion, not a synchronisation device:
    remaining blocked is a property that only exists over an interval, and the window spans ~20 of the
    waiter's 100ms polls, so a waiter that gives up on the retry cannot slip through it.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_drop_retry_follow_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    mv_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    cleanup_lock_path = f"{keeper_path}/cleanup_lock"
    num_failing_files = 5

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,
            "failed_files_ttl_sec": 0,
            "tracked_files_limit": 0,
        },
    )

    invalid_csv = b"not,valid,numbers\n"
    for i in range(num_failing_files):
        put_s3_file_content(started_cluster, f"{files_path}/failed_{i}.csv", invalid_csv)

    create_mv(node, table_name, dst_table_name)

    def failed_znodes():
        result = node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    def wait_for(predicate, timeout_sec=120):
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.5)
        return False

    assert wait_for(
        lambda: failed_znodes() >= num_failing_files
    ), f"expected {num_failing_files} failed znodes, got {failed_znodes()}"

    node.query(f"DROP TABLE {mv_name}")

    zk = started_cluster.get_kazoo_client("zoo1")

    def take_cleanup_lock():
        try:
            zk.create(cleanup_lock_path, TEST_DROP_LOCK_VALUE, ephemeral=True)
            return True
        except NodeExistsError:
            return False

    assert wait_for(
        take_cleanup_lock, timeout_sec=60
    ), "could not acquire cleanup_lock for the test"

    drop_result = {}

    def run_drop():
        try:
            node.query(f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}")
        except Exception as e:  # noqa: BLE001 - reported through the assertions below
            drop_result["error"] = e

    drop_thread = threading.Thread(target=run_drop)
    drop_thread.start()
    try:
        waiting_message = (
            f"{keeper_path}): Another replica is executing "
            f"SYSTEM DROP S3QUEUE FAILED FILES"
        )
        assert wait_for(
            lambda: node.contains_in_log(waiting_message)
        ), "drop command did not reach the loser branch"

        # The attempt loses its session and the statement retries: same command, new lock node, and
        # `/failed` untouched because the new attempt has only just started. Done as one transaction so
        # the waiter cannot observe the path empty and mistake that for the command finishing.
        transaction = zk.transaction()
        transaction.delete(cleanup_lock_path)
        transaction.create(cleanup_lock_path, TEST_DROP_LOCK_VALUE, ephemeral=True)
        results = transaction.commit()
        assert all(
            not isinstance(r, Exception) for r in results
        ), f"the retry handoff transaction did not commit: {results}"

        assert failed_znodes() == num_failing_files, (
            "the retry must begin with /failed still full - otherwise this test would pass even for a "
            "waiter that gave up and fell back to the emptiness check"
        )

        # The regression assertion: the waiter must still be waiting for its command.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            assert drop_thread.is_alive(), (
                "the waiter stopped waiting when its own command retried - it should have recognised "
                "the same command id and kept waiting for the verdict still to come"
            )
            time.sleep(0.1)

        # The retry now completes and publishes under the same command id.
        for child in zk.get_children(failed_path):
            zk.delete(f"{failed_path}/{child}")
        zk.create(
            f"{keeper_path}/last_drop_result",
            json.dumps(
                {
                    "command_id": TEST_DROP_COMMAND_ID,
                    "attempt_id": str(zk.exists(cleanup_lock_path).czxid),
                    "success": True,
                    "snapshot_size": num_failing_files,
                    "deleted": num_failing_files,
                    "error": "",
                }
            ).encode(),
        )
        zk.delete(cleanup_lock_path)
    finally:
        drop_thread.join(timeout=300)

    assert not drop_thread.is_alive(), "drop command did not return after the retry published its result"
    assert "error" not in drop_result, (
        f"waiter rejected the verdict of the command it was waiting for: {drop_result.get('error')}"
    )
    assert failed_znodes() == 0, f"failed znodes remain: {failed_znodes()}"

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(f"DROP TABLE IF EXISTS {dst_table_name}")


def test_legacy_metadata_inherits_failed_files_ttl_from_tracked_ttl(started_cluster):
    """A table whose Keeper metadata predates `failed_files_ttl_sec` keeps its old cleanup behaviour.

    Before this setting existed, `/failed` was trimmed by `tracked_file_ttl_sec`. Metadata written
    then has no `failed_files_ttl_sec` key at all, so the parser falls back to `tracked_files_ttl_sec`
    for it - that fallback is the PR's backward-compatibility promise, and it has to survive a
    restart or re-attach without the user setting anything.

    Asserted on the `adjustFromKeeper` log rather than on cleanup behaviour, and deliberately so: the
    count-based tracked-files sweep now also trims `/failed` for every non-exclusive mode using
    `tracked_files_ttl_sec`, so a behavioural test would pass whether or not the fallback worked -
    the file would be removed either way. The log line proves the parsed value specifically: it is
    emitted only when Keeper's value differs from the local one and the local one was never set,
    which is exactly the legacy case, and it carries the value that was inherited.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_legacy_ttl_{uuid.uuid4().hex[:8]}"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    tracked_ttl = 3

    # `failed_files_ttl_sec` is deliberately not set: this table looks like one created before the
    # setting existed, which is what makes the local value "never explicitly set".
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "tracked_file_ttl_sec": tracked_ttl,
        },
    )

    zk = started_cluster.get_kazoo_client("zoo1")
    metadata_path = f"{keeper_path}/metadata"

    # Rewrite the metadata node as a pre-upgrade server would have written it: with the key absent
    # entirely, not merely set to zero. Absence is what triggers the fallback.
    raw, _ = zk.get(metadata_path)
    metadata = json.loads(raw.decode())
    assert "failed_files_ttl_sec" in metadata, (
        f"expected the current server to write the key, so its removal is meaningful: {metadata}"
    )
    assert int(metadata["tracked_files_ttl_sec"]) == tracked_ttl, metadata
    del metadata["failed_files_ttl_sec"]
    zk.set(metadata_path, json.dumps(metadata).encode())

    # Re-attach so the metadata is parsed again from Keeper.
    node.query(f"DETACH TABLE {table_name}")
    node.query(f"ATTACH TABLE {table_name}")

    # The inherited value, reported by `adjustFromKeeper`: Keeper says `tracked_ttl`, the local
    # setting was never set, so the table adopts Keeper's. Had the fallback not applied, Keeper would
    # have parsed as 0, matched the local 0, and this line would never be emitted.
    #
    # The grep pattern deliberately omits the backticks the message puts around the setting name.
    # `contains_in_log` and `grep_in_log` interpolate the pattern into a double-quoted string that is
    # then run by `bash -c`, where a backtick opens a command substitution: the setting name would be
    # executed as a command and replaced by nothing, leaving a pattern that can never match. So grep
    # for the backtick-free part and match the whole line in Python, where backticks are just text.
    expected = f"Using `failed_files_ttl_sec` from keeper: {tracked_ttl} (local: 0)"
    reported = [
        line
        for line in node.grep_in_log("from keeper: ").splitlines()
        if "failed_files_ttl_sec" in line
    ]
    assert any(expected in line for line in reported), (
        f"expected the legacy fallback to be reported in the log: {expected!r}\n"
        "`from keeper` lines for this setting instead:\n" + "\n".join(reported[-10:])
    )

    node.query(f"DROP TABLE {table_name}")
