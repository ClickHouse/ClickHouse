import logging
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_mv,
    create_table,
    generate_random_files,
    generate_random_string,
    put_s3_file_content,
)

AVAILABLE_MODES = ["unordered", "ordered"]

@pytest.fixture(autouse=True)
def s3_queue_setup_teardown(started_cluster):
    instance = started_cluster.instances["instance"]
    instance.query("DROP DATABASE IF EXISTS default; CREATE DATABASE default;")

    minio = started_cluster.minio_client
    for obj in minio.list_objects(started_cluster.minio_bucket, recursive=True):
        minio.remove_object(started_cluster.minio_bucket, obj.object_name)

    yield


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()

def _run_flush_in_thread(node, table_name, file_path):
    """Start FLUSH in a daemon thread; return (thread, done_event, error_list)."""
    done = threading.Event()
    errors = []

    def _target():
        try:
            node.query(
                f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name}"
                f" PATH '{file_path}'"
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            done.set()

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    return t, done, errors


def _wait_for_flush_running(node, table_name, timeout_s=10):
    """
    Poll system.processes until the FLUSH query for `table_name` is visible.
    This is a deterministic signal that the query has reached the server and
    registered its Keeper watches — unlike a fixed sleep it does not depend on
    timing.
    """
    for _ in range(timeout_s * 10):
        count = int(node.query(
            f"SELECT count() FROM system.processes "
            f"WHERE query LIKE '%FLUSH OBJECT STORAGE QUEUE%{table_name}%'"
        ))
        if count > 0:
            return
        time.sleep(0.1)
    raise AssertionError(
        f"FLUSH for {table_name} did not appear in system.processes within {timeout_s} s"
    )


def _wait_for_commit_failure(node, table_name, timeout_s=60):
    """
    Spin until the background thread has hit `fail_commit` at least once.
    The log message proves the worker has the file, has inserted the data,
    and is now stuck failing to write the `Processed` node to Keeper.
    """
    msg = (
        f"StorageS3Queue (default.{table_name}): Failed to process data:"
        f" Code: 999. Coordination::Exception: Failed to commit processed files"
    )
    for _ in range(timeout_s):
        if node.contains_in_log(msg):
            return
        time.sleep(1)
    raise AssertionError(
        f"Background thread did not hit fail_commit within {timeout_s} s"
    )

@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_blocks_until_commit_succeeds(started_cluster, mode):
    """
    The `object_storage_queue_fail_commit` failpoint pins the background worker
    in a permanent retry loop — the file can never reach `Processed` state while
    the failpoint is active.  FLUSH must therefore block for as long as the
    failpoint is enabled, and unblock as soon as the commit is allowed to proceed.

    The assertion that FLUSH is blocking is made after `system.processes` confirms
    the query is running on the server, which is a deterministic signal that the
    Keeper watches have been registered.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_block_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/test_0.csv"

    generate_random_files(started_cluster, files_path, count=1, row_num=5)

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    try:
        create_table(
            started_cluster,
            node,
            table_name,
            mode,
            files_path,
            additional_settings={"keeper_path": keeper_path},
        )
        create_mv(node, table_name, dst_table_name)

        # Wait until the background thread has hit the failpoint at least once.
        # This proves the worker has the file but cannot commit it.
        _wait_for_commit_failure(node, table_name)

        # Start FLUSH while the failpoint is still active.
        t, flush_done, flush_errors = _run_flush_in_thread(node, table_name, file_path)

        # Wait until the FLUSH query is visible in system.processes — this confirms
        # the query is running on the server and its Keeper watches are registered.
        _wait_for_flush_running(node, table_name)

        # fail_commit is still active so the file cannot be committed; FLUSH must
        # be blocking at this point.
        assert not flush_done.is_set(), (
            "FLUSH returned while fail_commit was still active — it did not block"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    # With the failpoint gone the next commit will succeed; FLUSH must unblock.
    assert flush_done.wait(timeout=120), (
        "FLUSH did not unblock within 120 s after disabling fail_commit"
    )
    t.join()

    if flush_errors:
        raise flush_errors[0]

    # The file was reinserted on every failed retry, so count may exceed row_num;
    # we only assert that at least one successful processing cycle occurred.
    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count > 0, f"Expected rows in destination table after flush, got 0"



@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_returns_quickly_if_already_processed(started_cluster, mode):
    """
    If the file was already processed before FLUSH is called, the command must
    return well within the watch-timeout period (< 5 s).
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_already_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"

    generate_random_files(started_cluster, files_path, count=1, row_num=3)
    file_path = f"{files_path}/test_0.csv"

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={"keeper_path": keeper_path},
    )
    create_mv(node, table_name, dst_table_name)

    # Wait for the background thread to process the file naturally.
    for _ in range(60):
        if int(node.query(f"SELECT count() FROM {dst_table_name}")) == 3:
            break
        time.sleep(1)
    else:
        raise AssertionError("File was not processed within 60 s")

    # FLUSH must see `Processed` on the first Keeper read and return immediately.
    deadline = time.monotonic() + 5.0
    node.query(
        f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'"
    )
    assert time.monotonic() < deadline, (
        "SYSTEM FLUSH took too long for an already-processed file"
    )


@pytest.mark.parametrize("mode", AVAILABLE_MODES)
def test_flush_raises_on_failed_path(started_cluster, mode):
    """
    When the background thread permanently fails a file (retries exhausted),
    FLUSH must raise an exception (error code ABORTED) instead of waiting forever.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_fail_{mode}_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/bad.csv"

    # Upload a file whose first column cannot be parsed as UInt32.
    put_s3_file_content(started_cluster, file_path, b"not_a_number,1,2\n")

    create_table(
        started_cluster,
        node,
        table_name,
        mode,
        files_path,
        additional_settings={
            "keeper_path": keeper_path,
            "s3queue_loading_retries": 0,
        },
    )
    create_mv(node, table_name, dst_table_name)

    # Wait until Keeper shows the file as permanently failed.
    zk = started_cluster.get_kazoo_client("zoo1")
    for _ in range(60):
        try:
            if zk.get_children(f"{keeper_path}/failed/"):
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise AssertionError("File was not marked as failed within 60 s")

    error = node.query_and_get_error(
        f"SYSTEM FLUSH OBJECT STORAGE QUEUE default.{table_name} PATH '{file_path}'"
    )
    assert error, "Expected FLUSH to raise an error for a permanently failed file"
    assert "ABORTED" in error or "failed" in error.lower(), (
        f"Unexpected error message: {error}"
    )


def test_flush_ordered_with_buckets(started_cluster):
    """
    FLUSH must work correctly for ordered-mode queues that use multiple buckets.
    Each bucket has its own processed pointer and the watch must land on the right
    per-bucket node.  `object_storage_queue_fail_commit` pins the worker so FLUSH
    is guaranteed to be blocking when the assertion is made.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_buckets_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/test_0.csv"

    generate_random_files(started_cluster, files_path, count=1, row_num=4)

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    try:
        create_table(
            started_cluster,
            node,
            table_name,
            "ordered",
            files_path,
            additional_settings={
                "keeper_path": keeper_path,
                "buckets": 4,
            },
        )
        create_mv(node, table_name, dst_table_name)

        _wait_for_commit_failure(node, table_name)

        t, flush_done, flush_errors = _run_flush_in_thread(node, table_name, file_path)
        _wait_for_flush_running(node, table_name)

        assert not flush_done.is_set(), (
            "FLUSH returned while fail_commit was still active — it did not block"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    assert flush_done.wait(timeout=120), (
        "FLUSH (buckets=4) did not unblock within 120 s after disabling fail_commit"
    )
    t.join()

    if flush_errors:
        raise flush_errors[0]

    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count > 0, f"Expected rows in destination table after flush, got 0 (buckets=4)"



def test_flush_ordered_with_hive_partitioning(started_cluster):
    """
    FLUSH must be watch-driven in ordered mode with HIVE partitioning.

    In the HIVE/REGEX partitioned case the `Processed` marker lives under
    `processed/<partition_key>`, not the parent `processed/` node.  A ZooKeeper
    watch on a parent does NOT fire when a child is created or modified, so FLUSH
    must watch the child directly.

    This test verifies that the watch is placed on the correct node by using
    `fail_commit` to pin the worker, confirming FLUSH blocks, then releasing and
    confirming FLUSH unblocks.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_hive_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    partition_subpath = "date=2021-01-01/city=NYC"
    file_path = f"{files_path}/{partition_subpath}/test_0.csv"

    put_s3_file_content(
        started_cluster,
        file_path,
        b"1,2,3\n4,5,6\n7,8,9\n",
    )

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    try:
        create_table(
            started_cluster,
            node,
            table_name,
            "ordered",
            files_path,
            hive_partitioning_path=f"{partition_subpath}/",
            hive_partitioning_columns="date Date, city String",
            additional_settings={"keeper_path": keeper_path},
        )
        create_mv(node, table_name, dst_table_name, virtual_columns="date Date, city String")

        _wait_for_commit_failure(node, table_name)

        t, flush_done, flush_errors = _run_flush_in_thread(node, table_name, file_path)
        _wait_for_flush_running(node, table_name)

        assert not flush_done.is_set(), (
            "FLUSH returned while fail_commit was still active — it did not block"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    assert flush_done.wait(timeout=120), (
        "FLUSH (HIVE partitioned) did not unblock within 120 s after disabling fail_commit"
    )
    t.join()

    if flush_errors:
        raise flush_errors[0]

    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count > 0, f"Expected rows in destination table after flush, got 0"


def test_flush_ordered_with_regex_partitioning(started_cluster):
    """
    FLUSH must be watch-driven in ordered mode with REGEX partitioning.

    In the REGEX partitioned case the `Processed` marker lives under
    `processed/<partition_key>`, not the parent `processed/` node.  A ZooKeeper
    watch on a parent does NOT fire when a child is created or modified, so FLUSH
    must watch the child directly.

    This test uses hostname-based regex partitioning: filenames match the pattern
    `(?P<hostname>[^_]+)_<timestamp>_<sequence>.csv` and the `hostname` group is
    the partition key.  For `server-1_20251217T100000.000000Z_0001.csv` the
    Keeper processed pointer lives at `processed/server-1`.

    The watch correctness is verified via `fail_commit`: FLUSH must block while
    the failpoint is active and unblock as soon as it is disabled.
    """
    node = started_cluster.instances["instance"]
    table_name = f"flush_regex_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    files_path = f"{table_name}_data"
    keeper_path = f"/clickhouse/test_{table_name}"
    file_path = f"{files_path}/server-1_20251217T100000.000000Z_0001.csv"

    put_s3_file_content(
        started_cluster,
        file_path,
        b"1,2,3\n4,5,6\n7,8,9\n",
    )

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_fail_commit")
    try:
        create_table(
            started_cluster,
            node,
            table_name,
            "ordered",
            files_path,
            partitioning_mode="regex",
            partition_regex=r"(?P<hostname>[^_]+)_(?P<timestamp>\d{8}T\d{6}\.\d{6}Z)_(?P<sequence>\d+)",
            partition_component="hostname",
            additional_settings={"keeper_path": keeper_path},
        )
        create_mv(node, table_name, dst_table_name)

        _wait_for_commit_failure(node, table_name)

        t, flush_done, flush_errors = _run_flush_in_thread(node, table_name, file_path)
        _wait_for_flush_running(node, table_name)

        assert not flush_done.is_set(), (
            "FLUSH returned while fail_commit was still active — it did not block"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_fail_commit")

    assert flush_done.wait(timeout=120), (
        "FLUSH (REGEX partitioned) did not unblock within 120 s after disabling fail_commit"
    )
    t.join()

    if flush_errors:
        raise flush_errors[0]

    count = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert count > 0, f"Expected rows in destination table after flush, got 0"
