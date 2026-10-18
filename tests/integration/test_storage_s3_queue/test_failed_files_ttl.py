import logging
import time
import re
import json
import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

def _plant_retriable_markers(zk, failed_path, count):
    planted = []
    for i in range(count):
        node_name = f"dummy_{i}.retriable"
        zk.create(f"{failed_path}/{node_name}", b"", makepath=True)
        planted.append(node_name)
    return planted

from helpers.s3_queue_common import (
    generate_random_files,
    put_s3_file_content,
    create_table,
    create_mv,
)

@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "instance",
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            user_configs=["configs/users.xml"],
            with_zookeeper=True,
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "instance2",
            main_configs=[
                "configs/s3queue_log.xml",
                "configs/remote_servers.xml",
            ],
            user_configs=["configs/users.xml"],
            with_zookeeper=True,
            with_minio=True,
            stay_alive=True,
        )
        cluster.start()
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


def test_tracked_files_limit_ignores_a_retriable_backlog(started_cluster):
    """A backlog of `.retriable` markers must not evict a terminal failure the cap should keep.

    The count cap sized its removal budget from the raw child count of `/failed`, which holds both
    terminal nodes and `.retriable` markers, while the deletion loop only ever walks terminal nodes -
    markers are excluded on purpose, because deleting one resets the retry counter it carries and a
    file that should have been given up on would be retried forever.

    So the budget was spent on nodes that were never candidates. With the limit at 3, five markers
    and one terminal node, the raw count asked for three removals, the only eligible node was that
    single real failure, and it was deleted while all five markers stayed. The cap was enforced
    against nothing and cost a real failure its record.

    Nothing here is removable - one terminal node is under the limit of three - so a correct sweep
    deletes nothing at all, however many markers sit beside it.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_limit_retriable_backlog_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"

    tracked_files_limit = 3
    retriable_backlog = 5
    cleanup_interval_ms = 3000

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

    put_s3_file_content(
        started_cluster, f"{files_path}/bad_one.csv", b"invalid,data,here\n"
    )

    create_mv(node, table_name, dst_table_name)

    def failed_znode_names():
        result = node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return set(result.split("\n")) if result else set()

    def terminal_znodes():
        return {n for n in failed_znode_names() if not n.endswith(".retriable")}

    def retriable_znodes():
        return {n for n in failed_znode_names() if n.endswith(".retriable")}

    terminal_ready = False
    for _ in range(60):
        if len(terminal_znodes()) == 1:
            terminal_ready = True
            break
        time.sleep(1)

    assert terminal_ready, f"expected one terminal failed node, got {terminal_znodes()}"
    the_terminal_node = next(iter(terminal_znodes()))

    zk = started_cluster.get_kazoo_client("zoo1")
    planted = _plant_retriable_markers(zk, failed_path, retriable_backlog)

    # The raw child count is now 6 against a limit of 3, so a sweep that counts markers believes it
    # must remove three and finds exactly one node it is allowed to touch.
    assert len(failed_znode_names()) > tracked_files_limit, (
        "the raw child count must exceed the limit, or this test cannot tell the two behaviours "
        f"apart: {failed_znode_names()}"
    )

    # Several sweep intervals, so this is not just "the sweep has not run yet".
    deadline = time.monotonic() + 6 * cleanup_interval_ms / 1000
    while time.monotonic() < deadline:
        assert the_terminal_node in terminal_znodes(), (
            "the terminal failed node was evicted by a backlog of .retriable markers that were "
            "never candidates for removal - the cap counted nodes it cannot delete"
        )
        time.sleep(0.5)

    assert retriable_znodes() == set(planted), (
        f"the sweep must never touch .retriable markers, got {retriable_znodes()}"
    )

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
    failed_path = f"{keeper_path}/failed"

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
            "tracked_files_limit": 0,
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

    def get_keeper_failed_children_count():
        return int(node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip())

    # Wait for file to fail (up to 60 seconds)
    for _ in range(60):
        if get_failed_count() > 0:
            break
        time.sleep(1)

    # Assert file is marked as failed, both in the in-memory cache and in Keeper
    failed_count = get_failed_count()
    assert failed_count > 0, "Invalid file should be marked as Failed"
    keeper_failed_count = get_keeper_failed_children_count()
    assert keeper_failed_count > 0, "Failed file should have a /failed znode in Keeper"

    # Wait past the TTL period
    time.sleep(5)

    # In ordered mode, TTL cleanup is disabled, so the failed file should still be
    # there both in the in-memory cache AND as an actual znode in Keeper - checking
    # only the cache would still pass even if failed_files_ttl_sec incorrectly
    # deleted the real Keeper node, since the cache is not refreshed here.
    failed_count_after = get_failed_count()
    assert failed_count_after == failed_count, \
        "Failed files should NOT be cleaned up in ordered mode (cleanup_failed_files is disabled)"
    keeper_failed_count_after = get_keeper_failed_children_count()
    assert keeper_failed_count_after == keeper_failed_count, \
        "Failed file's znode should NOT be removed from Keeper in ordered mode"

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


def test_success_clears_stale_retriable_marker(started_cluster):
    """A file that fails a few times (leaving a live `.retriable` marker with a nonzero
    retry count) and then succeeds must have that marker cleared as part of the same
    success. Left behind, it would resurface with its stale retry count if the path
    is ever reprocessed later (e.g. after /processed expires via TTL/limit), and could
    even reject a fresh claim outright if loading_retries has since been lowered.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_success_clears_retriable_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    file_name = "flaky_then_ok.csv"

    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 5,
            "polling_min_timeout_ms": 1000,
            "polling_max_timeout_ms": 1000,
        },
    )

    # Invalid content: the file will fail and accumulate a live `.retriable` marker.
    put_s3_file_content(started_cluster, f"{files_path}/{file_name}", b"not,valid,data\n")

    create_mv(node, table_name, dst_table_name)

    def retriable_retries():
        result = node.query(
            f"SELECT value FROM system.zookeeper WHERE path = '{failed_path}' "
            f"AND name LIKE '%.retriable'"
        ).strip()
        for line in result.split("\n"):
            if not line:
                continue
            match = re.search(r'"retries"\s*:\s*(\d+)', line)
            if match:
                return int(match.group(1))
        return None

    # Wait until at least one failure has happened and left a live .retriable marker.
    retries_before_fix = None
    for _ in range(60):
        retries_before_fix = retriable_retries()
        if retries_before_fix is not None and retries_before_fix >= 1:
            break
        time.sleep(1)
    assert retries_before_fix is not None and retries_before_fix >= 1, (
        f"expected a live .retriable marker with retries >= 1 before overwriting the "
        f"file, got: {retries_before_fix}"
    )

    # Overwrite with valid content: the next retry attempt will succeed.
    put_s3_file_content(started_cluster, f"{files_path}/{file_name}", b"1,2,3\n")

    processed_ready = False
    for _ in range(60):
        result = node.query(
            f"SELECT count() FROM {dst_table_name}"
        ).strip()
        if result and int(result) > 0:
            processed_ready = True
            break
        time.sleep(1)
    assert processed_ready, "file never succeeded after being fixed"

    # The .retriable marker must be gone now - cleared atomically with success.
    remaining = node.query(
        f"SELECT name FROM system.zookeeper WHERE path = '{failed_path}' "
        f"AND name LIKE '%.retriable'"
    ).strip()
    assert remaining == "", (
        f"stale .retriable marker(s) survived a successful reprocess: {remaining}"
    )

    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_ordered_mode_non_max_processed_file_clears_stale_retriable_marker(started_cluster):
    """In ordered mode, only a bucket's max-processed file (by lexicographic path,
    within a single commit batch) goes through prepareProcessedRequestsImpl, which
    clears a live `.retriable` marker. Other Processed files in the same bucket go
    through prepareResetProcessingRequests instead, which previously never cleared
    `.retriable`, leaking a stale retry-count node forever for any successful file
    that happens not to be its bucket's max path in the batch that commits it.

    Forces both files into bucket 0 via `buckets: 1`. `a_flaky.csv` fails first
    (alone, in its own batch) leaving a live `.retriable` marker, then is fixed and
    committed in the same batch as `z_valid.csv` - whose lexicographically larger
    path makes it the bucket's max-processed file, pushing `a_flaky.csv` through the
    non-max (reset) branch this fix targets.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_ordered_non_max_retriable_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    flaky_file = "a_flaky.csv"
    valid_file = "z_valid.csv"

    create_table(
        started_cluster,
        node,
        table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "buckets": 1,
            "s3queue_loading_retries": 5,
            "polling_min_timeout_ms": 1000,
            "polling_max_timeout_ms": 1000,
        },
    )

    # Invalid content: this file will fail alone and accumulate a live `.retriable` marker.
    put_s3_file_content(started_cluster, f"{files_path}/{flaky_file}", b"not,valid,data\n")

    create_mv(node, table_name, dst_table_name)

    def retriable_retries():
        result = node.query(
            f"SELECT value FROM system.zookeeper WHERE path = '{failed_path}' "
            f"AND name LIKE '%.retriable'"
        ).strip()
        for line in result.split("\n"):
            if not line:
                continue
            match = re.search(r'"retries"\s*:\s*(\d+)', line)
            if match:
                return int(match.group(1))
        return None

    retries_before_fix = None
    for _ in range(60):
        retries_before_fix = retriable_retries()
        if retries_before_fix is not None and retries_before_fix >= 1:
            break
        time.sleep(1)
    assert retries_before_fix is not None and retries_before_fix >= 1, (
        f"expected a live .retriable marker with retries >= 1 before overwriting the "
        f"file, got: {retries_before_fix}"
    )

    # Fix the flaky file AND add a lexicographically larger file at the same time,
    # so the next poll commits both in the same batch - making z_valid.csv the
    # bucket's max-processed file and pushing a_flaky.csv through the non-max branch.
    put_s3_file_content(started_cluster, f"{files_path}/{flaky_file}", b"1,2,3\n")
    put_s3_file_content(started_cluster, f"{files_path}/{valid_file}", b"4,5,6\n")

    processed_ready = False
    for _ in range(60):
        result = node.query(f"SELECT count() FROM {dst_table_name}").strip()
        if result and int(result) >= 2:
            processed_ready = True
            break
        time.sleep(1)
    assert processed_ready, "both files never succeeded after fixing the flaky one"

    # The .retriable marker for the non-max (flaky) file must be gone now.
    remaining = node.query(
        f"SELECT name FROM system.zookeeper WHERE path = '{failed_path}' "
        f"AND name LIKE '%.retriable'"
    ).strip()
    assert remaining == "", (
        f"stale .retriable marker(s) survived a successful non-max-file reprocess: {remaining}"
    )

    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_ordered_mode_rejects_mismatched_tracked_files_limit(started_cluster):
    """Two ordered-mode tables attaching to the same keeper_path with different
    tracked_files_limit values must fail to both attach, since tracked_files_limit
    also gates /failed cleanup in ordered mode (not just unordered), so replicas
    must agree on it - otherwise failed-node eviction timing would depend on which
    replica wins the cleanup lock race rather than on shared Keeper metadata.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_ordered_limit_mismatch_{uuid.uuid4().hex[:8]}"
    other_table_name = f"{table_name}_other"
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
            "tracked_files_limit": 100,
        },
    )

    error = create_table(
        started_cluster,
        node,
        other_table_name,
        "ordered",
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "tracked_files_limit": 200,
        },
        expect_error=True,
    )
    assert "tracked_files_limit" in error, \
        f"Expected metadata mismatch error mentioning tracked_files_limit, got: {error}"

    node.query(f"DROP TABLE {table_name}")


def test_lowering_loading_retries_is_honored_same_process_hot_cache(started_cluster):
    """Test that lowering `s3queue_loading_retries` mid-retry is honored immediately
    in the SAME process, without a restart - i.e. when the in-memory FileStatus cache
    is already hot (state=Failed) rather than cold (state=None).

    Regression for: trySetProcessing()/prepareSetProcessingRequests()'s `state ==
    Failed` branch revalidated against Keeper's live retry count and correctly denied
    a new processing attempt once `keeper_retries >= max_loading_retries`, but never
    called tryTerminalizeExhaustedRetriableMarker() before returning. So an exhausted
    `.retriable` marker observed via this hot-cache path stayed a `.retriable` node
    forever: not retryable, and invisible to both `failed_files_ttl_sec` and `SYSTEM
    DROP S3QUEUE FAILED FILES` (which intentionally skip `.retriable` nodes). This is
    the same underlying gap as the cold-cache/after-restart case covered by
    test_lowering_loading_retries_is_honored_after_restart, but reached via a
    different code path (state == Failed hot-cache branch vs. state == None fresh-file
    branch), which is why a restart is deliberately NOT performed here.

    Steps:
    1. A file fails repeatedly with `s3queue_loading_retries` set high, so it does not
       reach terminal state on its own; wait until Keeper's `.retriable` marker shows
       retries == 2. By this point the in-memory FileStatus cache for this file is
       already state=Failed (set locally after each retriable failure).
    2. Lower `s3queue_loading_retries` to 2 via ALTER TABLE ... MODIFY SETTING, while
       the live `.retriable` marker (retries=2) is still present in Keeper, and while
       the in-memory cache remains hot (no restart).
    3. Confirm the file does not get granted another processing attempt, and that the
       `.retriable` marker eventually becomes a terminal `/failed/<hash>` node.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_lower_retries_hot_cache_{uuid.uuid4().hex[:8]}"
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
            "s3queue_loading_retries": 100,
            "failed_files_ttl_sec": 0,
            "polling_min_timeout_ms": 15000,
            "polling_max_timeout_ms": 15000,
        },
    )

    invalid_csv = b"not,valid,data\n"
    put_s3_file_content(
        started_cluster, f"{files_path}/bad_lower_retries_hot.csv", invalid_csv
    )

    create_mv(node, table_name, dst_table_name)

    def get_retry_count_from_keeper():
        failed_path = f"{keeper_path}/failed"
        result = node.query(
            f"SELECT name, value FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()

        if not result:
            return None, False

        import re

        for line in result.split("\n"):
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            node_name, node_value = parts
            file_path_match = re.search(r'"file_path"\s*:\s*"([^"]*)"', node_value)
            if not file_path_match or "bad_lower_retries_hot.csv" not in file_path_match.group(1):
                continue
            is_terminal = not node_name.endswith(".retriable")
            match = re.search(r'"retries"\s*:\s*(\d+)', node_value)
            if match:
                return int(match.group(1)), is_terminal
        return None, False

    logging.info("Waiting for the live .retriable marker to reach retries == 2...")
    timeout = 60
    for elapsed in range(timeout):
        time.sleep(1)
        retries, is_terminal = get_retry_count_from_keeper()
        if retries is not None:
            logging.info(f"[{elapsed}s] Retry count: {retries}, terminal: {is_terminal}")
            if not is_terminal and retries >= 2:
                break
    else:
        pytest.fail(
            f"TIMEOUT: .retriable marker did not reach retries >= 2 within {timeout}s "
            f"(last observed: {get_retry_count_from_keeper()})"
        )

    retries_before_lowering, terminal_before_lowering = get_retry_count_from_keeper()
    assert not terminal_before_lowering, (
        "Precondition failed: file already reached terminal state before the "
        "setting was lowered - the test did not exercise the intended race."
    )
    logging.info(
        f"Live .retriable marker observed with retries={retries_before_lowering}. "
        f"Lowering s3queue_loading_retries to 2 WITHOUT restarting (hot-cache path)..."
    )

    # Deliberately no restart here: the in-memory FileStatus cache for this file
    # should already be state=Failed at this point (set locally after each retriable
    # failure), so the next scheduling pass exercises the hot-cache `state == Failed`
    # branch of trySetProcessing()/prepareSetProcessingRequests(), not the cold-cache
    # `state == None` branch that the after-restart test covers.
    node.query(f"ALTER TABLE {table_name} MODIFY SETTING s3queue_loading_retries=2")

    logging.info("Waiting to confirm no extra processing attempt is granted, and the "
                 "marker eventually becomes terminal, without a restart...")
    max_retries_seen = retries_before_lowering
    final_retries, final_is_terminal = get_retry_count_from_keeper()
    for elapsed in range(60):
        time.sleep(1)
        retries, is_terminal = get_retry_count_from_keeper()
        if retries is not None:
            if retries > max_retries_seen:
                max_retries_seen = retries
            final_retries, final_is_terminal = retries, is_terminal
            if is_terminal:
                break

    logging.info(
        f"Final: retries={final_retries}, terminal={final_is_terminal}, "
        f"max_retries_seen={max_retries_seen}"
    )

    assert max_retries_seen <= 2, (
        f"BUG: file was granted an extra processing attempt in the same process - "
        f"retry count increased from {retries_before_lowering} to {max_retries_seen} "
        f"even though s3queue_loading_retries was lowered to 2. This means the hot "
        f"in-memory cache (state=Failed) was not revalidated against Keeper's live "
        f"retry count."
    )

    assert final_is_terminal, (
        f"BUG: the exhausted .retriable marker (retries={final_retries}) was never "
        f"terminalized into a terminal /failed/<hash> node via the hot-cache "
        f"(state == Failed) path. The file is blocked from further processing but "
        f"remains invisible to both failed_files_ttl_sec and SYSTEM DROP S3QUEUE "
        f"FAILED FILES, which intentionally skip .retriable nodes."
    )


def test_lowering_loading_retries_is_honored_after_restart(started_cluster):
    """Test that lowering `s3queue_loading_retries` mid-retry is honored immediately
    after a restart, even though the in-memory retry cache is cold.

    Regression for: getPathState() only probed the terminal failed node, so a file
    whose in-memory FileStatus was fresh (state=None, e.g. right after a restart or
    on a different replica) skipped Keeper revalidation entirely in
    trySetProcessing()/prepareSetProcessingRequests(). If Keeper already held a live
    `.retriable` marker whose stored retry count met or exceeded a *newly lowered*
    `s3queue_loading_retries`, the file would still be granted one extra processing
    attempt instead of being immediately treated as exhausted.

    Steps:
    1. A file fails repeatedly with `s3queue_loading_retries` set high, so it does not
       reach the terminal state on its own; wait until Keeper's `.retriable` marker
       shows retries == 2.
    2. Lower `s3queue_loading_retries` to 2 via ALTER TABLE ... MODIFY SETTING, while
       the live `.retriable` marker (retries=2) is still present in Keeper.
    3. Restart the ClickHouse instance - this wipes the in-memory FileStatus cache,
       reproducing the cold-cache/cross-replica scenario.
    4. After the restart, the file must not be granted another processing attempt:
       the retry count must not exceed 2, and the file must end up in the terminal
       Failed state without incrementing past the newly lowered limit.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_lower_retries_restart_{uuid.uuid4().hex[:8]}"
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
            # High enough that the file does not reach terminal failure on its own
            # before we lower the limit below.
            "s3queue_loading_retries": 100,
            "failed_files_ttl_sec": 0,  # Disabled: avoid a TTL sweep racing with this test.
            "polling_min_timeout_ms": 15000,
            "polling_max_timeout_ms": 15000,
        },
    )

    invalid_csv = b"not,valid,data\n"
    put_s3_file_content(
        started_cluster, f"{files_path}/bad_lower_retries.csv", invalid_csv
    )

    create_mv(node, table_name, dst_table_name)

    def get_retry_count_from_keeper():
        """Parse the retriable node's stored retry count from Keeper, if present."""
        failed_path = f"{keeper_path}/failed"
        result = node.query(
            f"SELECT name, value FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()

        if not result:
            return None, False

        import re

        for line in result.split("\n"):
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            node_name, node_value = parts
            file_path_match = re.search(r'"file_path"\s*:\s*"([^"]*)"', node_value)
            if not file_path_match or "bad_lower_retries.csv" not in file_path_match.group(1):
                continue
            is_terminal = not node_name.endswith(".retriable")
            match = re.search(r'"retries"\s*:\s*(\d+)', node_value)
            if match:
                return int(match.group(1)), is_terminal
        return None, False

    logging.info("Waiting for the live .retriable marker to reach retries == 2...")
    timeout = 60
    for elapsed in range(timeout):
        time.sleep(1)
        retries, is_terminal = get_retry_count_from_keeper()
        if retries is not None:
            logging.info(f"[{elapsed}s] Retry count: {retries}, terminal: {is_terminal}")
            if not is_terminal and retries >= 2:
                break
    else:
        pytest.fail(
            f"TIMEOUT: .retriable marker did not reach retries >= 2 within {timeout}s "
            f"(last observed: {get_retry_count_from_keeper()})"
        )

    retries_before_lowering, terminal_before_lowering = get_retry_count_from_keeper()
    assert not terminal_before_lowering, (
        "Precondition failed: file already reached terminal state before the "
        "setting was lowered - the test did not exercise the intended race."
    )
    logging.info(
        f"Live .retriable marker observed with retries={retries_before_lowering}. "
        f"Lowering s3queue_loading_retries to 2 and restarting..."
    )

    node.query(f"ALTER TABLE {table_name} MODIFY SETTING s3queue_loading_retries=2")

    # Restart wipes the in-memory FileStatus cache for every file, reproducing the
    # cold-cache scenario: on the next scheduling pass, the file's FileStatus starts
    # as state=None with retries=0, so the fix must revalidate against Keeper's live
    # retry count rather than trusting (or ignoring) the reset-to-zero local cache.
    node.restart_clickhouse()

    # Give the scheduler a few polling cycles to re-evaluate the file after restart.
    logging.info("Waiting after restart to confirm no extra processing attempt is granted...")
    max_retries_seen_after_restart = retries_before_lowering
    for elapsed in range(30):
        time.sleep(1)
        retries, is_terminal = get_retry_count_from_keeper()
        if retries is not None:
            if retries > max_retries_seen_after_restart:
                max_retries_seen_after_restart = retries
            if is_terminal:
                break

    final_retries, final_is_terminal = get_retry_count_from_keeper()
    logging.info(
        f"After restart: retries={final_retries}, terminal={final_is_terminal}, "
        f"max_retries_seen_after_restart={max_retries_seen_after_restart}"
    )

    assert max_retries_seen_after_restart <= 2, (
        f"BUG: file was granted an extra processing attempt after restart - retry "
        f"count increased from {retries_before_lowering} to {max_retries_seen_after_restart} "
        f"even though s3queue_loading_retries was lowered to 2 before the restart. "
        f"This means the cold in-memory cache (state=None after restart) was not "
        f"revalidated against Keeper's live retry count."
    )

    # Regression for a second bug in the same scenario: blocking the file from a new
    # attempt (above) is not enough on its own. Only a *new* failed attempt can convert
    # a `.retriable` marker into a terminal `/failed/<hash>` node, and this marker was
    # never allowed a new attempt - so without an explicit terminalization path, the file
    # stayed in `.retriable` forever: not retryable (blocked by the assertion above) and
    # not cleanable, since both `failed_files_ttl_sec` and `SYSTEM DROP S3QUEUE FAILED
    # FILES` intentionally skip `.retriable` nodes. Wait past the fix's poll window and
    # confirm the file actually reaches the terminal state instead of staying stuck.
    if not final_is_terminal:
        for elapsed in range(30):
            time.sleep(1)
            final_retries, final_is_terminal = get_retry_count_from_keeper()
            if final_is_terminal:
                break

    assert final_is_terminal, (
        f"BUG: the exhausted .retriable marker (retries={final_retries}) was never "
        f"terminalized into a /failed/<hash> node after the limit was lowered below its "
        f"retry count. The file is permanently stuck: blocked from further processing "
        f"attempts, but invisible to failed_files_ttl_sec and SYSTEM DROP S3QUEUE FAILED "
        f"FILES, which both skip .retriable nodes."
    )

    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_legacy_metadata_inherits_failed_files_ttl_from_tracked_ttl(started_cluster):
    """A table whose Keeper metadata predates `failed_files_ttl_sec` keeps its old cleanup behaviour.

    Before this setting existed, `/failed` was trimmed by `tracked_file_ttl_sec`. Metadata written
    then has no `failed_files_ttl_sec` key at all, so the parser falls back to `tracked_files_ttl_sec`
    for it - that fallback is the PR's backward-compatibility promise, and it has to survive a
    restart or re-attach without the user setting anything.

    Checked two ways. First, the `adjustFromKeeper` log line: proof of the parsed value specifically,
    since it is emitted only when Keeper's value differs from the local one and the local one was
    never set - exactly the legacy case - and it carries the inherited value. Second, a real failed
    file is put through the table after the re-attach and left past the inherited TTL: with
    `tracked_files_limit` pinned to 0, the count-based tracked-files sweep (which independently trims
    `/failed` using `tracked_files_ttl_sec` in non-exclusive mode - see
    `test_tracked_file_ttl_sec_does_not_expire_failed_files`) cannot also account for the removal, so
    the terminal znode disappearing is attributable only to `failed_files_ttl_sec` cleanup running
    with the inherited value, which only happens if the fallback actually took effect.
    """
    node = started_cluster.instances["instance"]

    table_name = f"test_legacy_ttl_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    tracked_ttl = 3
    cleanup_interval_ms = 2000

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
            # Isolates the terminal-znode-expiry check below from the count-based tracked-files
            # sweep, which trims `/failed` on its own schedule and would otherwise confound it.
            "tracked_files_limit": 0,
            "cleanup_interval_min_ms": cleanup_interval_ms,
            "cleanup_interval_max_ms": cleanup_interval_ms,
            "s3queue_loading_retries": 0,
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

    # Now confirm the inherited value actually drives cleanup, not just the log line.
    put_s3_file_content(
        started_cluster, f"{files_path}/bad_file.csv", b"invalid,data,here\n"
    )
    create_mv(node, table_name, dst_table_name)

    def terminal_failed_znodes():
        result = node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        names = result.split("\n") if result else []
        return {name for name in names if not name.endswith(".retriable")}

    terminal_ready = False
    for _ in range(60):
        if terminal_failed_znodes():
            terminal_ready = True
            break
        time.sleep(1)
    assert terminal_ready, "expected the invalid file to reach terminal failure"

    # Past the inherited TTL, with several cleanup sweeps to run.
    time.sleep(tracked_ttl + 4 * cleanup_interval_ms / 1000)

    assert terminal_failed_znodes() == set(), (
        "the terminal failed znode survived past the inherited failed_files_ttl_sec - "
        "the legacy fallback value is not actually driving cleanup"
    )

    node.query(f"DROP TABLE {table_name}")
    node.query(f"DROP TABLE {dst_table_name}")


def test_wait_for_path_reads_loading_retries_live(started_cluster):
    """`waitForPathToBeProcessed()` must react to `ALTER TABLE ... MODIFY SETTING s3queue_loading_retries`
    made while a `SYSTEM FLUSH OBJECT STORAGE QUEUE ... PATH` wait is already in progress, not to a
    retry-limit value captured when the wait started - AND it must distinguish an already-terminal
    `/failed/<hash>` node from a live `.retriable` marker while doing so.

    The file is failed with `s3queue_loading_retries=0`, so `prepareFailedRequestsImpl` creates a
    terminal `/failed/<hash>` node directly (no `.retriable` marker at all - see the `retriable =
    max_loading_retries != 0` condition), and the background streaming loop never touches it again -
    no ongoing retry race to control for.

    The pause failpoint then parks the wait right before its terminality check. While parked, the
    limit is *raised* to 5. A terminal `/failed/<hash>` node is permanent and must never be treated
    as retryable again, no matter how high `s3queue_loading_retries` is later raised - so the wait
    must still raise ABORTED immediately once unparked, exactly as it would have before the ALTER.
    If the terminality check incorrectly re-applied the live retry-limit comparison to this terminal
    node (instead of only to a live `.retriable` marker), it would wrongly keep waiting past query
    timeout instead of raising ABORTED right away.
    """
    node = started_cluster.instances["instance"]
    table_name = f"test_wait_live_retries_{uuid.uuid4().hex[:8]}"
    dst_table_name = f"{table_name}_dst"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"
    failed_path = f"{keeper_path}/failed"
    file_name = "bad_one.csv"

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

    put_s3_file_content(started_cluster, f"{files_path}/{file_name}", b"invalid,data,here\n")

    create_mv(node, table_name, dst_table_name)

    def failed_znode_count():
        result = node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}'"
        ).strip()
        return int(result) if result else 0

    failed_ready = False
    for _ in range(60):
        if failed_znode_count() == 1:
            failed_ready = True
            break
        time.sleep(1)
    assert failed_ready, (
        "expected the file to reach a terminal Failed state (loading_retries=0 means "
        "terminal on the first failure, with no .retriable marker created at all)"
    )

    # Sanity: confirm this is genuinely the terminal-node path, not a live .retriable marker -
    # otherwise this test would silently stop exercising the terminal case it's meant to cover.
    retriable_count = node.query(
        f"SELECT count() FROM system.zookeeper WHERE path = '{failed_path}' "
        f"AND name LIKE '%.retriable'"
    ).strip()
    assert retriable_count == "0", (
        "expected no live .retriable marker with loading_retries=0 - the file should have "
        "gone straight to a terminal /failed/<hash> node"
    )

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_pause_before_wait_retry_check")

    flush_errors = []

    def run_flush():
        try:
            node.query(
                f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} "
                f"PATH '{files_path}/{file_name}'"
            )
        except Exception as exc:
            flush_errors.append(exc)

    flush_thread = threading.Thread(target=run_flush)
    flush_thread.start()

    node.query(
        "SYSTEM WAIT FAILPOINT object_storage_queue_pause_before_wait_retry_check PAUSE"
    )

    # Raise the limit while parked - well past the file's zero recorded retries. This must NOT
    # resurrect the terminal node as retryable.
    node.query(f"ALTER TABLE {table_name} MODIFY SETTING s3queue_loading_retries=5")
    node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_pause_before_wait_retry_check")

    flush_thread.join(timeout=30)
    assert not flush_thread.is_alive(), (
        "SYSTEM FLUSH did not return after the terminality check was unparked - a terminal "
        "/failed/<hash> node must raise ABORTED immediately regardless of a later-raised "
        "s3queue_loading_retries, not keep waiting"
    )
    assert len(flush_errors) == 1, f"expected exactly one error, got: {flush_errors}"
    assert "failed to be processed" in str(flush_errors[0]), (
        f"expected an ABORTED 'failed to be processed' error for the terminal node, "
        f"got: {flush_errors[0]}"
    )

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(f"DROP TABLE IF EXISTS {dst_table_name}")
