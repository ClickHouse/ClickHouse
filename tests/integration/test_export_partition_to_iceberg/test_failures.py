import time

from helpers.export_partition_helpers import (
    make_iceberg_s3,
    make_source,
    unique_suffix,
    wait_for_exception_count,
    wait_for_export_status,
    wait_for_export_to_start,
)
from helpers.iceberg_export_stats import fetch_manifest_entries
from helpers.network import PartitionManager

from .common import (
    data_file_partition_records,
    partition_scalar,
    setup_tables,
)

CLUSTER_INSTANCES = ["replica1", "replica2"]

# `EXPORT PARTITION` into Iceberg under injected failure: commit-path failpoints, retryable errors,
# stopped moves, killed and timed-out tasks, and the guarantee that a failed commit leaves the
# destination snapshot intact. Slow and timing sensitive - kept out of the parallel batch.


def test_failure_is_logged_in_system_table(cluster, source_engine):
    """
    When a part export fails with a non-retryable error the export must be marked
    FAILED in system.partition_exports with a non-zero exception_count.

    Uses the export_part_non_retryable_throw failpoint (throws BAD_ARGUMENTS, a
    denylisted code) so the task fails fast without consuming any timeout budget.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query("SYSTEM ENABLE FAILPOINT export_part_non_retryable_throw")
    try:
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
            settings={"allow_insert_into_iceberg": 1},
        )

        # short timeout to exercise the fast fail path for non retryable errors
        wait_for_export_status(node, mt_table, iceberg_table, "2020", "FAILED", timeout=20)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT export_part_non_retryable_throw")

    status = node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert status == "FAILED", f"Expected FAILED status, got: {status!r}"

    exception_count = int(node.query(
        f"""
        SELECT any(exception_count) FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip())
    assert exception_count > 0, "Expected non-zero exception_count in system.partition_exports"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after a failed export, got {count}"


def test_inject_short_living_failures(cluster):
    """
    Transient S3 failures must not prevent the export from completing: after the
    network is restored the export should retry and eventually land COMPLETED.
    """
    node = cluster.instances["replica1"]
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"],
                 s3_retry_attempts=1)

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} SETTINGS allow_insert_into_iceberg = 1")

    with PartitionManager() as pm:
        pm.add_rule({
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })
        pm.add_rule({
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })

        node.query(f"SYSTEM START MOVES {mt_table}")

        # Let at least one retry happen before restoring the network.
        time.sleep(15)

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows after retry, got {count}"

    status = node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert status == "COMPLETED", f"Expected COMPLETED in system table, got: {status!r}"

    exception_count = int(node.query(
        f"""
        SELECT exception_count FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip())
    assert exception_count >= 1, "Expected at least one transient exception to be recorded"


def test_export_partition_retryable_error_killed_on_timeout(cluster, source_engine):
    """
    A retryable part-export error (here FAULT_INJECTED via export_part_retryable_throw)
    must NOT fail the task on a retry budget: there is no retry budget anymore, so the
    part keeps retrying until the absolute task timeout fires and the task is KILLED.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query("SYSTEM ENABLE FAILPOINT export_part_retryable_throw")
    try:
        # Under the old budget model a small retry budget would fail the task after the
        # first retry. With the new model there is no budget and only the 5s timeout fails it.
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
            f" SETTINGS export_merge_tree_partition_task_timeout_seconds = 5,"
            f"          allow_insert_into_iceberg = 1"
        )

        # Give the scheduler time to attempt and fail the part several times. The old
        # budget would already have transitioned the task to FAILED by now.
        time.sleep(15)
        status = node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{iceberg_table}'"
            f"   AND partition_id = '2020'"
        ).strip()
        assert status != "FAILED", (
            f"Retryable failures must not fail the task on a budget, got status {status!r}"
        )

        # The timeout (5s) is past; KILLED fires on the next manifest-updater poll cycle.
        wait_for_export_status(
            node, mt_table, iceberg_table, "2020", "KILLED", timeout=90
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT export_part_retryable_throw")

    exception_count = int(node.query(
        f"SELECT any(exception_count) FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}'"
        f"   AND destination_table = '{iceberg_table}'"
        f"   AND partition_id = '2020'"
    ).strip())
    assert exception_count > 0, "Expected at least one retryable exception to be recorded"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after a killed export, got {count}"


def test_export_partition_retryable_error_recovers_after_failpoint_cleared(cluster, source_engine):
    """
    A retryable part-export error must keep the task PENDING (not FAILED) while the
    failure persists, applying a per-replica back-off between attempts. Once the
    failure clears the export completes successfully — proving the back-off only
    spaces retries out and never permanently blocks progress.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query("SYSTEM ENABLE FAILPOINT export_part_retryable_throw")
    try:
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
            f" SETTINGS export_merge_tree_partition_retry_initial_backoff_seconds = 1,"
            f"          export_merge_tree_partition_retry_max_backoff_seconds = 2,"
            f"          allow_insert_into_iceberg = 1"
        )

        # Wait until at least one retryable failure has been recorded; the task must
        # still be PENDING (retrying), never FAILED.
        wait_for_exception_count(node, mt_table, iceberg_table, "2020",
                                 min_exception_count=1, timeout=60)
        status = node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{iceberg_table}'"
            f"   AND partition_id = '2020'"
        ).strip()
        assert status == "PENDING", (
            f"Retryable failures must keep the task PENDING, got status {status!r}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT export_part_retryable_throw")

    # With the failpoint cleared the next retry succeeds and the export completes.
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED", timeout=90)

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows after recovery, got {count}"


def test_export_partition_local_backoff_does_not_block_other_replica(cluster):
    """
    Back-off is per-replica and in-memory: a part that one replica keeps failing on
    (and therefore puts into its local back-off) must NOT be prevented from being
    exported by another replica. This is the whole reason the back-off is local
    rather than distributed in ZooKeeper.

    replica1 is given a persistent *retryable* failure (export_part_retryable_throw)
    and is the only replica scheduling at first (moves are stopped on replica2). Once
    replica1 has recorded a failure and a local back-off entry, replica2's scheduler
    is enabled. Because the failpoint stays active on replica1 the whole time, the
    only way the export can reach COMPLETED is replica2 picking up the very part that
    replica1 keeps failing — proving the back-off does not leak across replicas.
    """
    replica1 = cluster.instances["replica1"]
    replica2 = cluster.instances["replica2"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1", "replica2"])

    # Phase 1: only replica1 schedules. Stop the export scheduler on replica2 so the
    # part is guaranteed to be attempted (and fail) on replica1 first.
    replica2.query(f"SYSTEM STOP MOVES {mt_table}")

    replica1.query("SYSTEM ENABLE FAILPOINT export_part_retryable_throw")
    try:
        replica1.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
            f" SETTINGS export_merge_tree_partition_retry_initial_backoff_seconds = 1,"
            f"          export_merge_tree_partition_retry_max_backoff_seconds = 2,"
            f"          allow_insert_into_iceberg = 1"
        )

        # replica1 attempts the part, fails (retryable), and enters local back-off.
        # The task must stay PENDING — there is no retry budget to fail it.
        wait_for_exception_count(replica1, mt_table, iceberg_table, "2020",
                                 min_exception_count=1, timeout=60)

        wait_for_export_status(replica1, mt_table, iceberg_table, "2020", "PENDING", timeout=60)

        # The back-off entry must be observable on replica1 (the failing replica).
        deadline = time.time() + 90
        backoff_replica1 = "0"
        while time.time() < deadline:
            backoff_replica1 = replica1.query(
                f"SELECT length(local_backoff_per_part) FROM system.partition_exports"
                f" WHERE source_table = '{mt_table}'"
                f"   AND destination_table = '{iceberg_table}'"
                f"   AND partition_id = '2020'"
            ).strip()
            if backoff_replica1 not in ("", "0"):
                break
            time.sleep(0.5)
        assert backoff_replica1 not in ("", "0"), (
            "Expected replica1 to carry a local back-off entry for the failing part, "
            f"got {backoff_replica1!r}"
        )

        # ... and it must NOT have leaked to replica2, which never attempted the part.
        # This is the core assertion: local back-off state is not shared across replicas.
        backoff_replica2 = replica2.query(
            f"SELECT length(local_backoff_per_part) FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{iceberg_table}'"
            f"   AND partition_id = '2020'"
        ).strip()

        assert backoff_replica2 in ("", "0"), (
            f"replica2 must not carry replica1's local back-off, got {backoff_replica2!r}"
        )

        # Phase 2: enable replica2's scheduler. replica1 keeps failing (the failpoint
        # is still active), so completion can only come from replica2 exporting the
        # part that replica1 is backing off on.
        replica2.query(f"SYSTEM START MOVES {mt_table}")

        wait_for_export_status(replica2, mt_table, iceberg_table, "2020", "COMPLETED", timeout=60)
    finally:
        replica1.query("SYSTEM DISABLE FAILPOINT export_part_retryable_throw")

    count = int(replica2.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows after replica2 completed the export, got {count}"


def test_export_partition_scheduler_skipped_when_moves_stopped(cluster, source_engine):
    """
    Verify that selectPartsToExport() skips the scheduler entirely when moves
    are stopped (moves_blocker guard at the top of the function).

    No ZK locks are acquired and no background tasks are submitted, so the
    Iceberg table must remain empty across multiple scheduler cycles.  Once moves
    are re-enabled the export completes and rows appear in the Iceberg table.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )

    wait_for_export_to_start(node, mt_table, iceberg_table, "2020")

    # Wait for several scheduler cycles (each fires every 5 s).
    # If the guard is absent the scheduler would run and rows would appear in the Iceberg table.
    time.sleep(12)

    status = node.query(
        f"SELECT status FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}' AND destination_table = '{iceberg_table}'"
        f" AND partition_id = '2020'"
    ).strip()

    assert status == "PENDING", f"Expected PENDING while moves are stopped, got '{status}'"

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table while scheduler is skipped, got {count}"

    node.query(f"SYSTEM START MOVES {mt_table}")

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export completed, got {count}"


def test_export_partition_resumes_after_stop_moves(cluster, source_engine):
    """
    Verify that SYSTEM STOP MOVES before EXPORT PARTITION does not permanently
    orphan the ZooKeeper part lock for Iceberg destinations.

    When moves are stopped the scheduler still picks parts up and submits them to
    the background executor, but ExportPartTask::isCancelled() returns true (via
    moves_blocker), causing QUERY_WAS_CANCELLED before any data is written.  The
    fix in handlePartExportFailure must release the ZK lock so the part is retried
    once moves are restarted.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
        f" SETTINGS allow_insert_into_iceberg = 1"
    )

    wait_for_export_to_start(node, mt_table, iceberg_table, "2020")

    # Give the scheduler enough time to attempt (and cancel) the part task at least once.
    time.sleep(5)

    status = node.query(
        f"SELECT status FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}' AND destination_table = '{iceberg_table}'"
        f" AND partition_id = '2020'"
    ).strip()
    assert status == "PENDING", f"Expected PENDING while moves are stopped, got '{status}'"

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table while moves are stopped, got {count}"

    node.query(f"SYSTEM START MOVES {mt_table}")

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export completed, got {count}"


def test_export_partition_resumes_after_stop_moves_during_export(cluster, source_engine):
    """
    Verify that SYSTEM STOP MOVES issued while an Iceberg export is actively
    retrying (S3 blocked) does not permanently orphan the ZooKeeper part lock.
    """
    node = cluster.instances["replica1"]
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
        f" SETTINGS allow_insert_into_iceberg = 1")

    wait_for_export_to_start(node, mt_table, iceberg_table, "2020")

    with PartitionManager() as pm:
        pm.add_rule({
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })
        pm.add_rule({
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })

        node.query(f"SYSTEM STOP MOVES {mt_table}")

        time.sleep(3)

        status = node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}' AND destination_table = '{iceberg_table}'"
            f" AND partition_id = '2020'"
        ).strip()
        assert status == "PENDING", (
            f"Expected PENDING while moves are stopped and S3 is blocked, got '{status}'"
        )

        node.query(f"SYSTEM START MOVES {mt_table}")

    # MinIO is now unblocked; the next scheduler cycle should succeed.
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export completed, got {count}"


def test_export_data_files_are_not_cleaned_up_on_commit_failure(cluster):
    """
    Verify that a commit failure does not delete the already-written data files.
    `cleanup` only removes the manifest entry / manifest list, never the data files
    (a peer replica might still commit the same transaction). This guards against
    data loss / dangling references.

    The iceberg_writes_non_retry_cleanup failpoint throws BAD_ARGUMENTS while writing
    the manifest entry, after the data files have been written. BAD_ARGUMENTS is a
    non-retryable error code, so the task transitions to FAILED; we then confirm the
    exported data files are still physically present in object storage by reading
    them directly (the Iceberg manifests were removed by cleanup, so we glob the raw
    parquet data files instead).
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"
    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query("SYSTEM ENABLE FAILPOINT iceberg_writes_non_retry_cleanup")
    try:
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
            settings={"allow_insert_into_iceberg": 1},
        )
        # BAD_ARGUMENTS from the commit phase is non-retryable -> the task fails fast.
        wait_for_export_status(node, mt_table, iceberg_table, "2020", "FAILED", timeout=60)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT iceberg_writes_non_retry_cleanup")

    # The data files were written before the commit failure; cleanup must have left
    # them intact. Read them straight from object storage (bypassing the Iceberg
    # metadata, which cleanup removed) and confirm all 3 exported rows survive.
    rows = int(node.query(
        f"SELECT count() FROM s3("
        f"'http://minio1:9001/root/data/{iceberg_table}/**.parquet', "
        f"'minio', 'ClickHouse_Minio_P@ssw0rd', 'Parquet')"
    ).strip())
    assert rows == 3, (
        f"Expected the 3 exported rows to still exist as data files after a failed "
        f"commit (data files must not be cleaned up), got {rows}"
    )


def test_post_publish_exception_preserves_snapshot(cluster):
    """
    Regression test for the post-publish exception-safety bug in
    commitImportPartitionTransactionImpl.

    Before the fix, any exception thrown after the Iceberg snapshot was published
    (e.g. from metadata-cache invalidation) would fall through to the outer
    `catch (...)` and invoke `cleanup(false)`, which unconditionally removed the
    manifest entry and manifest list referenced by the just-published snapshot.
    A subsequent read would then fail because the live snapshot points to deleted
    files.

    The failpoint `iceberg_writes_post_publish_throw` is placed inside the
    post-publish region (after both the metadata file is written and
    `published = true` is set). With the fix in place:
      - the commit stays durable (snapshot is readable, manifests are intact);
      - the export is marked COMPLETED because the outer `catch (...)` sees
        `published == true` and returns the populated commit info with the real
        paths produced by this attempt (no retry needed);
      - all exported rows are visible through the Iceberg table.
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"
    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query("SYSTEM ENABLE FAILPOINT iceberg_writes_post_publish_throw")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, (
        f"Snapshot must remain readable after a post-publish exception, "
        f"expected 3 rows but got {count} (manifest files likely deleted by "
        f"over-broad cleanup)"
    )

    result = node.query(
        f"SELECT id, year FROM {iceberg_table} WHERE year = 2020 ORDER BY id"
    ).strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", (
        f"Unexpected data after post-publish exception recovery:\n{result}"
    )

    # After a post-publish exception the catch handler with published==true returns
    # the populated commit info (real metadata / manifest list / manifest file paths).
    # ExportPartitionUtils::commit persists it to the commit_info znode, so the system
    # table should show a real metadata path here, not the already-committed sentinel.
    committed_metadata_file = node.query(
        f"""
        SELECT committed_metadata_file FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert committed_metadata_file, (
        "committed_metadata_file should be populated after a successful post-publish-catch return"
    )
    assert not committed_metadata_file.startswith("<"), (
        f"committed_metadata_file should be a real metadata path, got the already-committed sentinel: {committed_metadata_file!r}"
    )
    assert committed_metadata_file.endswith(".metadata.json"), (
        f"Expected a *.metadata.json path in committed_metadata_file, got: {committed_metadata_file!r}"
    )


def test_export_task_timeout_kills_stuck_pending_task(cluster):
    """
    Verify that export_merge_tree_partition_task_timeout_seconds auto-kills a task
    that remains PENDING past the deadline, transitioning it to KILLED with a
    descriptive last_exception.

    The export_partition_commit_always_throw failpoint wedges the task in the
    commit retry loop (REGULAR failpoint, fires on every commit attempt) with a
    retryable error, so the task never fails on its own and the timeout branch in
    tryCleanup is the actual mechanism under test.

    Replicated-only: the failpoint lives in `ExportPartitionUtils::commit`, the
    ZooKeeper-coordinated commit routine. A plain MergeTree commits through
    `MergeTreePartitionExportScheduler::tryCommit`, which the failpoint does not reach, so the
    export simply completes and there is nothing for the timeout to kill.
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"
    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query("SYSTEM ENABLE FAILPOINT export_partition_commit_always_throw")

    try:
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
            f" SETTINGS export_merge_tree_partition_task_timeout_seconds = 5,"
            f"          allow_insert_into_iceberg = 1"
        )

        # Timeout budget must cover: the 5s task timeout + one manifest-updating
        # poll cycle (~30s) + watch propagation. 90s is safe.
        wait_for_export_status(
            node, mt_table, iceberg_table, "2020",
            expected_status="KILLED",
            timeout=90,
        )

        # The KILL transition writes a per-replica last_exception leaf in the same
        # ZK multi as the status flip; handleStatusChanges then mirrors it into
        # memory together with the status. Poll briefly to allow that watch ->
        # mirror hop. We use arrayJoin to flatten the per-replica array column;
        # any replica reporting the timeout reason is sufficient.
        deadline = time.time() + 30
        last_exception = ""
        while time.time() < deadline:
            last_exception = node.query(
                f"""
                SELECT arrayStringConcat(
                    arrayMap(x -> x.message, last_exception_per_replica),
                    '\\n'
                )
                FROM system.partition_exports
                WHERE source_table = '{mt_table}'
                  AND destination_table = '{iceberg_table}'
                  AND partition_id = '2020'
                """
            ).strip()
            if "timed out" in last_exception:
                break
            time.sleep(0.5)
        assert "timed out" in last_exception, (
            f"Expected last_exception_per_replica column to mention the timeout reason, got: {last_exception!r}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT export_partition_commit_always_throw")


def test_export_partition_commit_uses_exported_parts_not_new_inserts(cluster):
    """The deferred commit derives the Iceberg partition value only from the exact exported parts
    recorded in the manifest, never from parts inserted/merged into the source partition after
    scheduling. A month-partitioned source exports one day into a day-partitioned destination (a
    data-dependent acceptance); while the commit is wedged, an earlier day is inserted and merged in,
    so the only active part now spans both days with its min at the new day. The commit must still
    stamp the exported day (the exported part is found among Outdated parts by name), not the merged-in
    earlier day, so the metadata matches the exported data files.

    Replicated-only for the same reason as test_export_task_timeout_kills_stuck_pending_task: the
    commit failpoint this test wedges the commit with only exists on the ZooKeeper-coordinated
    commit path."""
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_commit_parts_{uid}"
    iceberg_table = f"iceberg_commit_parts_{uid}"

    make_source(node, mt_table, "id Int64, event_date Date", "toYYYYMM(event_date)", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, '2024-03-20'), (2, '2024-03-20')")
    make_iceberg_s3(node, iceberg_table, "id Int64, event_date Date",
                    partition_by="toRelativeDayNum(event_date)")

    exported_day = int(node.query("SELECT toRelativeDayNum(toDate('2024-03-20'))").strip())
    injected_day = int(node.query("SELECT toRelativeDayNum(toDate('2024-03-05'))").strip())

    node.query("SYSTEM ENABLE FAILPOINT export_partition_commit_always_throw")
    try:
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '202403' TO TABLE {iceberg_table}"
            f" SETTINGS allow_insert_into_iceberg = 1"
        )
        # The commit is attempted only after every part is exported, so a non-zero exception count
        # means the data files are written and the commit is now wedged by the failpoint.
        wait_for_exception_count(node, mt_table, iceberg_table, "202403", min_exception_count=1, timeout=90)

        # Insert an earlier day into the same month partition and merge: the merged active part spans
        # both days with min = the injected (earlier) day, while the exported part becomes Outdated.
        node.query(f"INSERT INTO {mt_table} VALUES (3, '2024-03-05')")
        node.query(f"OPTIMIZE TABLE {mt_table} PARTITION ID '202403' FINAL")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT export_partition_commit_always_throw")

    wait_for_export_status(node, mt_table, iceberg_table, "202403", "COMPLETED", timeout=90)

    # The exported data files hold only 2024-03-20; the metadata day must match them.
    query_id = f"commit_parts_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_days = {int(partition_scalar(p, "event_date")) for p in partitions}
    assert meta_days == {exported_day}, (
        f"Metadata day {meta_days} must equal the exported day {exported_day} (2024-03-20), "
        f"not the injected day {injected_day} (2024-03-05)."
    )

    assert int(node.query(f"SELECT count() FROM {iceberg_table}").strip()) == 2, (
        "Only the two exported rows must be present in the destination."
    )
