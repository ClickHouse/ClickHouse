import concurrent.futures
import logging
import threading
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

    # Cache may be cleared by TTL cleanup (cache-Keeper consistency guarantee from e5fc138),
    # but if it still exists, it must show "Failed" status
    if final_status is not None:
        assert final_status == "Failed", \
            f"If cache entry exists, status must be 'Failed', got: {final_status}"
    # else: cache already cleared by TTL — acceptable per cache-Keeper consistency guarantee

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

    The `ON CLUSTER` case is asserted for the denial only. `executeDDLQueryOnCluster`
    resolves the cluster before checking access, so the cluster has to exist for the
    denial to be attributable to the privilege rather than to an unknown cluster.
    Proving the `ON CLUSTER` success path additionally needs a second running replica
    holding the same table, which this single-node module does not provide.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_drop_priv_{uuid.uuid4().hex[:8]}"
    user_name = f"user_drop_priv_{uuid.uuid4().hex[:8]}"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )

    node.query(f"CREATE USER {user_name} IDENTIFIED WITH no_password")
    # The command resolves the table, so the user must be able to see it at all;
    # otherwise a failure could be UNKNOWN_TABLE rather than a privilege denial.
    node.query(f"GRANT SHOW TABLES ON default.{table_name} TO {user_name}")

    direct_query = f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name}"
    on_cluster_query = (
        f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name} ON CLUSTER cluster"
    )

    # 1. Denied without the privilege, direct form.
    assert "ACCESS_DENIED" in node.query_and_get_error(direct_query, user=user_name)

    # 2. Denied without the privilege, ON CLUSTER form. This exercises
    #    getRequiredAccessForDDLOnCluster(), a separate path from the
    #    checkAccess() call inside the interpreter.
    assert "ACCESS_DENIED" in node.query_and_get_error(on_cluster_query, user=user_name)

    # 3. Grant exactly the new privilege, table-scoped.
    node.query(
        f"GRANT SYSTEM DROP S3QUEUE FAILED FILES ON default.{table_name} TO {user_name}"
    )

    # 4. The direct form now succeeds. query() raises on any error, so reaching the
    #    next statement is itself the assertion.
    node.query(direct_query, user=user_name)

    # 5. The privilege is table-scoped and must not leak to a different table.
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
    assert "ACCESS_DENIED" in node.query_and_get_error(
        f"SYSTEM DROP S3QUEUE FAILED FILES default.{other_table_name}", user=user_name
    )

    # Cleanup
    node.query(f"DROP USER {user_name}")
    node.query(f"DROP TABLE {table_name}")
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

    # Only one replica needs to consume, so each file fails once.
    create_mv(node1, table_name, dst_table_name)

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

    # Both replicas must have observed the failures in their own cache first,
    # otherwise "cache is empty afterwards" would prove nothing on the replica
    # that never populated it.
    assert wait_for(
        lambda: cached_failed(node1) >= num_failing_files
    ), f"instance cache not populated: {cached_failed(node1)}"
    assert wait_for(
        lambda: cached_failed(node2) >= num_failing_files
    ), f"instance2 cache not populated: {cached_failed(node2)}"

    # Both replicas issue the ON CLUSTER drop at the same time. The barrier makes
    # the overlap deterministic without depending on sleep timing.
    query = f"SYSTEM DROP S3QUEUE FAILED FILES default.{table_name} ON CLUSTER cluster"
    barrier = threading.Barrier(2)

    def run(node):
        barrier.wait()
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
    node1.query(f"DROP TABLE IF EXISTS {dst_table_name}")
