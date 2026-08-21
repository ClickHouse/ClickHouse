import io
import json
import logging
import re
import time
from typing import NamedTuple

import pytest
from avro.datafile import DataFileReader
from avro.io import DatumReader

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import (
    first_partition_id,
    make_iceberg_s3,
    make_rmt,
    unique_suffix,
    wait_for_exception_count,
    wait_for_export_status,
    wait_for_export_to_start,
)
from helpers.iceberg_export_stats import (
    assert_exported_stats,
    fetch_manifest_entries,
)
from helpers.network import PartitionManager


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "replica1",
            main_configs=[
                "configs/allow_experimental_export_partition.xml",
                "configs/config.d/metadata_log.xml",
            ],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )
        cluster.add_instance(
            "replica2",
            main_configs=[
                "configs/allow_experimental_export_partition.xml",
                "configs/config.d/metadata_log.xml",
            ],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_tables_after_test(cluster):
    """Drop all tables in the default database after every test.

    Without this, ReplicatedMergeTree tables from completed tests remain alive and keep
    running ZooKeeper background threads.  With many tables alive simultaneously the
    ZooKeeper session becomes overwhelmed and subsequent tests start seeing
    operation-timeout / session-expired errors.
    """
    yield
    for instance_name, instance in cluster.instances.items():
        try:
            tables_str = instance.query(
                "SELECT name FROM system.tables WHERE database = 'default' FORMAT TabSeparated"
            ).strip()
            if not tables_str:
                continue
            for table in tables_str.split("\n"):
                table = table.strip()
                if table:
                    instance.query(f"DROP TABLE IF EXISTS default.`{table}` SYNC")
        except Exception as e:
            logging.warning(
                f"drop_tables_after_test: cleanup failed on {instance_name}: {e}"
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_replicated_mt(node, mt_table: str, replica_name: str):
    make_rmt(node, mt_table, "id Int64, year Int32", "year",
             replica_name=replica_name)


def create_iceberg_s3_table(node, iceberg_table: str, if_not_exists: bool = False,
                            s3_retry_attempts: int = 3):
    """Create (or attach to an existing) IcebergS3 table at a per-test MinIO prefix."""
    make_iceberg_s3(
        node, iceberg_table, "id Int64, year Int32",
        partition_by="year", if_not_exists=if_not_exists,
        s3_retry_attempts=s3_retry_attempts,
    )


def setup_tables(cluster, mt_table: str, iceberg_table: str, nodes: list | None = None,
                 s3_retry_attempts: int = 3):
    """
    Create the ReplicatedMergeTree table on the given nodes, insert data on the first
    node, wait for replication, then create the Iceberg destination table on each node.

    The Iceberg table is created on the first node (which initialises the S3 metadata).
    Subsequent nodes attach to the same path with IF NOT EXISTS.

    `nodes` defaults to ["replica1", "replica2"].
    """
    if nodes is None:
        nodes = ["replica1", "replica2"]

    instances = [cluster.instances[n] for n in nodes]
    primary = instances[0]

    for i, instance in enumerate(instances):
        create_replicated_mt(instance, mt_table, nodes[i])

    primary.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")
    for instance in instances[1:]:
        instance.query(f"SYSTEM SYNC REPLICA {mt_table}")

    create_iceberg_s3_table(primary, iceberg_table, s3_retry_attempts=s3_retry_attempts)
    for instance in instances[1:]:
        create_iceberg_s3_table(instance, iceberg_table, if_not_exists=True,
                                s3_retry_attempts=s3_retry_attempts)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_export_partition_to_iceberg(cluster):
    """
    Basic happy path: export a single partition and verify row count and content.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", (
        f"Unexpected data in Iceberg table:\n{result}"
    )


def _destination_paths_has_sync_failed_marker(node, source_table, dest_table, partition_id):
    """True when destination_file_paths contains the Keeper sync-failed marker value."""
    result = node.query(
        f"SELECT has(arrayFlatten(mapValues(destination_file_paths)), '<failed to read from zk>')"
        f" FROM system.replicated_partition_exports"
        f" WHERE source_table = '{source_table}'"
        f"   AND destination_table = '{dest_table}'"
        f"   AND partition_id = '{partition_id}'"
    ).strip()
    return result == "1"


def wait_for_destination_paths_sync_failed_marker(
    node, source_table, dest_table, partition_id, expect_marker, timeout=90, poll_interval=0.5
):
    """Wait until destination_file_paths does/does not contain the sync-failed marker.

    The in-memory mirror refreshes on the manifest-updater poll (~30s), so the
    default timeout allows at least one full cycle plus headroom.
    """
    start_time = time.time()
    last = None
    while time.time() - start_time < timeout:
        last = _destination_paths_has_sync_failed_marker(
            node, source_table, dest_table, partition_id
        )
        if last == expect_marker:
            return
        time.sleep(poll_interval)

    raise TimeoutError(
        f"destination_file_paths sync-failed marker did not become {expect_marker}"
        f" within {timeout}s (last={last})"
    )


def test_export_two_partitions_to_iceberg(cluster):
    """
    Export two partitions in a single ALTER TABLE statement and verify that both
    land in the Iceberg table with correct row counts.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query(
        f"""
        ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2020' TO TABLE {iceberg_table},
            EXPORT PARTITION ID '2021' TO TABLE {iceberg_table}
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, iceberg_table, "2021", "COMPLETED")

    count_2020 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    count_2021 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2021").strip())

    assert count_2020 == 3, f"Expected 3 rows for year=2020, got {count_2020}"
    assert count_2021 == 1, f"Expected 1 row for year=2021, got {count_2021}"


def test_export_partition_all_to_iceberg(cluster):
    """
    `ALTER TABLE ... EXPORT PARTITION ALL TO TABLE ...` schedules every active partition
    in one statement and exercises the Iceberg-specific destination compatibility checks
    (which are repeated per sub-call inside the loop).
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, iceberg_table, "2021", "COMPLETED")

    count_2020 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    count_2021 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2021").strip())

    assert count_2020 == 3, f"Expected 3 rows for year=2020, got {count_2020}"
    assert count_2021 == 1, f"Expected 1 row for year=2021, got {count_2021}"


def test_failure_is_logged_in_system_table(cluster):
    """
    When a part export fails with a non-retryable error the export must be marked
    FAILED in system.replicated_partition_exports with a non-zero exception_count.

    Uses the export_part_non_retryable_throw failpoint (throws BAD_ARGUMENTS, a
    denylisted code) so the task fails fast without consuming any timeout budget.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

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
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert status == "FAILED", f"Expected FAILED status, got: {status!r}"

    exception_count = int(node.query(
        f"""
        SELECT any(exception_count) FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip())
    assert exception_count > 0, "Expected non-zero exception_count in system.replicated_partition_exports"

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
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert status == "COMPLETED", f"Expected COMPLETED in system table, got: {status!r}"

    exception_count = int(node.query(
        f"""
        SELECT exception_count FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
        """
    ).strip())
    assert exception_count >= 1, "Expected at least one transient exception to be recorded"


def test_export_partition_retryable_error_killed_on_timeout(cluster):
    """
    A retryable part-export error (here FAULT_INJECTED via export_part_retryable_throw)
    must NOT fail the task on a retry budget: there is no retry budget anymore, so the
    part keeps retrying until the absolute task timeout fires and the task is KILLED.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

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
            f"SELECT status FROM system.replicated_partition_exports"
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
        f"SELECT any(exception_count) FROM system.replicated_partition_exports"
        f" WHERE source_table = '{mt_table}'"
        f"   AND destination_table = '{iceberg_table}'"
        f"   AND partition_id = '2020'"
    ).strip())
    assert exception_count > 0, "Expected at least one retryable exception to be recorded"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after a killed export, got {count}"


def test_export_partition_retryable_error_recovers_after_failpoint_cleared(cluster):
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

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

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
            f"SELECT status FROM system.replicated_partition_exports"
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
                f"SELECT length(local_backoff_per_part) FROM system.replicated_partition_exports"
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
            f"SELECT length(local_backoff_per_part) FROM system.replicated_partition_exports"
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


def test_export_partition_scheduler_skipped_when_moves_stopped(cluster):
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

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

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
        f"SELECT status FROM system.replicated_partition_exports"
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


def test_export_partition_resumes_after_stop_moves(cluster):
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

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
        f" SETTINGS allow_insert_into_iceberg = 1"
    )

    wait_for_export_to_start(node, mt_table, iceberg_table, "2020")

    # Give the scheduler enough time to attempt (and cancel) the part task at least once.
    time.sleep(5)

    status = node.query(
        f"SELECT status FROM system.replicated_partition_exports"
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


def test_export_partition_resumes_after_stop_moves_during_export(cluster):
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

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

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
            f"SELECT status FROM system.replicated_partition_exports"
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


def test_partition_transform_compatibility_accepted(cluster):
    """
    Verify that EXPORT PARTITION is accepted (no BAD_ARGUMENTS) for every
    supported transform when the MergeTree and Iceberg partition specs match.

    Cases covered:
    1. Compound identity (year, region), exported to a spec that lists the fields in reverse order
    2. Year transform  – toYearNumSinceEpoch(event_date)
    3. Month transform – toMonthNumSinceEpoch(event_date)
    4. truncate[4]     – icebergTruncate(4, category)
    5. bucket[8]       – icebergBucket(8, user_id)
    6. Compound mixed  – (toYearNumSinceEpoch(event_date), icebergBucket(16, user_id))
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()

    def check_accepted(mt, iceberg, description):
        pid = first_partition_id(node, mt)
        node.query(
            f"ALTER TABLE {mt} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}",
            settings={"allow_insert_into_iceberg": 1},
        )
        return pid

    # 1. Compound identity, with the destination listing the fields in the opposite order: the
    # source key pins both columns, so the partition is single-valued for either field order.
    cols = "id Int64, year Int32, region String"
    t = f"mt_acc_1_{uid}"; i = f"iceberg_acc_1_{uid}"
    make_rmt(node, t, cols, "(year, region)")
    node.query(f"INSERT INTO {t} VALUES (1, 2023, 'EU')")
    make_iceberg_s3(node, i, cols, "(region, year)")
    pid = check_accepted(t, i, "compound identity (year, region)")
    wait_for_export_status(node, t, i, pid, "COMPLETED")
    count = int(node.query(f"SELECT count() FROM {i}").strip())
    assert count == 1, f"[compound identity (year, region)] Expected 1 row in Iceberg table, got {count}"
    result = node.query(f"SELECT id, year, region FROM {i}").strip()
    assert result == "1\t2023\tEU", f"[compound identity (year, region)] Unexpected exported data:\n{result}"

    # 2. Year transform
    cols = "id Int64, event_date Date"
    t = f"mt_acc_2_{uid}"; i = f"iceberg_acc_2_{uid}"
    make_rmt(node, t, cols, "toYearNumSinceEpoch(event_date)")
    node.query(f"INSERT INTO {t} VALUES (1, '2020-06-15')")
    make_iceberg_s3(node, i, cols, "toYearNumSinceEpoch(event_date)")
    check_accepted(t, i, "year transform")

    # 3. Month transform
    cols = "id Int64, event_date Date"
    t = f"mt_acc_3_{uid}"; i = f"iceberg_acc_3_{uid}"
    make_rmt(node, t, cols, "toMonthNumSinceEpoch(event_date)")
    node.query(f"INSERT INTO {t} VALUES (1, '2020-06-15')")
    make_iceberg_s3(node, i, cols, "toMonthNumSinceEpoch(event_date)")
    check_accepted(t, i, "month transform")

    # 4. truncate[4]
    cols = "id Int64, category String"
    t = f"mt_acc_4_{uid}"; i = f"iceberg_acc_4_{uid}"
    make_rmt(node, t, cols, "icebergTruncate(4, category)")
    node.query(f"INSERT INTO {t} VALUES (1, 'clickhouse')")
    make_iceberg_s3(node, i, cols, "icebergTruncate(4, category)")
    check_accepted(t, i, "truncate[4]")

    # 5. bucket[8]
    cols = "id Int64, user_id Int64"
    t = f"mt_acc_5_{uid}"; i = f"iceberg_acc_5_{uid}"
    make_rmt(node, t, cols, "icebergBucket(8, user_id)")
    node.query(f"INSERT INTO {t} VALUES (1, 42)")
    make_iceberg_s3(node, i, cols, "icebergBucket(8, user_id)")
    check_accepted(t, i, "bucket[8]")

    # 6. Compound mixed: year(event_date) + bucket[16](user_id)
    cols = "id Int64, event_date Date, user_id Int64"
    t = f"mt_acc_6_{uid}"; i = f"iceberg_acc_6_{uid}"
    make_rmt(node, t, cols, "(toYearNumSinceEpoch(event_date), icebergBucket(16, user_id))")
    node.query(f"INSERT INTO {t} VALUES (1, '2021-03-01', 99)")
    make_iceberg_s3(node, i, cols, "(toYearNumSinceEpoch(event_date), icebergBucket(16, user_id))")
    check_accepted(t, i, "compound year+bucket[16]")


def test_partition_transform_compatibility_rejected(cluster):
    """
    Verify that partition specs that cannot be exported are rejected with BAD_ARGUMENTS.

    Acceptance is data-dependent: a source partition must map to a single Iceberg partition. The
    mismatch cases below therefore use data that makes the source partition span several
    destination partitions (a single-row partition would be trivially single-valued and accepted).

    Cases covered:
    1. Transform mismatch on the same column: year-transform source vs identity destination, where
       the year partition contains several distinct dates.
    2. Bucket count mismatch: bucket[8] vs bucket[16] (bucket is non-monotonic, always structural).
    3. Truncate width mismatch: truncate[4] source vs truncate[8] destination, with values sharing
       the 4-char prefix but differing within the first 8 chars.
    4. Unsupported MergeTree expression (intDiv) vs identity, with one bucket spanning several years.
    5. Destination partitions by a column that is not in the source partition key.
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()

    def assert_rejected(mt, iceberg, description):
        pid = first_partition_id(node, mt)
        error = node.query_and_get_error(
            f"ALTER TABLE {mt} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}",
            settings={"allow_insert_into_iceberg": 1},
        )
        assert "BAD_ARGUMENTS" in error, (
            f"[{description}] Expected BAD_ARGUMENTS, got: {error!r}"
        )

    # 1. Transform mismatch: MergeTree year-transform, Iceberg identity on same Date col
    cols = "id Int64, event_date Date"
    t = f"mt_rej_1_{uid}"; i = f"iceberg_rej_1_{uid}"
    make_rmt(node, t, cols, "toYearNumSinceEpoch(event_date)")
    node.query(f"INSERT INTO {t} VALUES (1, '2020-01-01'), (2, '2020-12-31')")
    make_iceberg_s3(node, i, cols, "event_date")   # identity, not year-transform
    assert_rejected(t, i, "year-transform source vs identity destination")

    # 2. Bucket count mismatch: bucket[8] vs bucket[16]
    cols = "id Int64, user_id Int64"
    t = f"mt_rej_2_{uid}"; i = f"iceberg_rej_2_{uid}"
    make_rmt(node, t, cols, "icebergBucket(8, user_id)")
    node.query(f"INSERT INTO {t} VALUES (1, 42)")
    make_iceberg_s3(node, i, cols, "icebergBucket(16, user_id)")
    assert_rejected(t, i, "bucket[8] vs bucket[16]")

    # 3. Truncate width mismatch: values share the 4-char prefix but differ within 8 chars.
    cols = "id Int64, category String"
    t = f"mt_rej_3_{uid}"; i = f"iceberg_rej_3_{uid}"
    make_rmt(node, t, cols, "icebergTruncate(4, category)")
    node.query(f"INSERT INTO {t} VALUES (1, 'clickhouse'), (2, 'clickfmt')")
    make_iceberg_s3(node, i, cols, "icebergTruncate(8, category)")
    assert_rejected(t, i, "truncate[4] source vs truncate[8] destination")

    # 4. Unsupported MergeTree expression vs identity: one intDiv bucket spans several years.
    cols = "id Int64, year Int32"
    t = f"mt_rej_4_{uid}"; i = f"iceberg_rej_4_{uid}"
    make_rmt(node, t, cols, "intDiv(year, 100)")
    node.query(f"INSERT INTO {t} VALUES (1, 2000), (2, 2099)")
    make_iceberg_s3(node, i, cols, "year")
    assert_rejected(t, i, "intDiv source vs identity destination")

    # 5. Destination partitions by a column absent from the source partition key.
    cols = "id Int64, year Int32"
    t = f"mt_rej_5_{uid}"; i = f"iceberg_rej_5_{uid}"
    make_rmt(node, t, cols, "year")
    node.query(f"INSERT INTO {t} VALUES (1, 2020)")
    make_iceberg_s3(node, i, cols, "id")   # identity on id, which the source does not partition by
    assert_rejected(t, i, "destination partitions by a non-source-key column")


def test_partition_key_compatibility_check(cluster):
    """
    Verify that EXPORT PARTITION throws BAD_ARGUMENTS synchronously when the
    MergeTree partition key does not match the Iceberg table's partition spec,
    and is accepted without error when the destination is satisfiable.

    Three cases:
    1. Column mismatch   – MergeTree PARTITION BY year, Iceberg PARTITION BY id (must be rejected)
    2. Unpartitioned dst – MergeTree PARTITION BY year, Iceberg unpartitioned (accepted: the source is
                           flattened into the single empty Iceberg partition)
    3. Matching keys     – both PARTITION BY year (must be accepted)
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"

    create_replicated_mt(node, mt_table, "replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2021)")
    node.query(f"SYSTEM SYNC REPLICA {mt_table}")

    # --- Case 1: Iceberg partitioned by 'id' but MergeTree by 'year' ---
    iceberg_col_mismatch = f"iceberg_col_mismatch_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_col_mismatch}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_col_mismatch}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        PARTITION BY id SETTINGS s3_retry_attempts = 3
        """
    )
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_col_mismatch}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition column mismatch, got: {error!r}"
    )

    # --- Case 2: Iceberg unpartitioned, MergeTree PARTITION BY year ---
    # An unpartitioned Iceberg table has a single (empty) partition, so a partitioned source is
    # flattened into it and the export is accepted; the partition-column values survive as data.
    iceberg_unpartitioned = f"iceberg_unpartitioned_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_unpartitioned}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_unpartitioned}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        SETTINGS s3_retry_attempts = 3
        """
    )
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_unpartitioned}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_unpartitioned, "2020", "COMPLETED")
    count = int(node.query(f"SELECT count() FROM {iceberg_unpartitioned}").strip())
    assert count == 2, f"Expected 2 rows in unpartitioned Iceberg table after export, got {count}"
    result = node.query(f"SELECT id, year FROM {iceberg_unpartitioned} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020", f"Unexpected data in unpartitioned Iceberg table:\n{result}"

    # --- Case 3: Matching partition keys (both PARTITION BY year) ---
    iceberg_match = f"iceberg_match_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_match}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_match}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        PARTITION BY year SETTINGS s3_retry_attempts = 3
        """
    )
    # Should not raise — the check passes so the export is accepted synchronously
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_match}",
        settings={"allow_insert_into_iceberg": 1},
    )


def test_partition_transform_equivalence_gate(cluster):
    """
    The Iceberg partition-compatibility gate accepts a source partition key whose transform is
    equivalent to (or finer than) the destination Iceberg transform when the exported partition is
    provably single-valued for every destination field, and rejects it otherwise. Accept cases are
    verified end-to-end (data + metadata); reject cases must throw BAD_ARGUMENTS synchronously.
    """
    node = cluster.instances["replica1"]
    dt = "id Int64, event_time DateTime"
    yr = "id Int64, year Int32, region String"

    cases = [
        # toDate -> day: rows within one day map to a single Iceberg day partition.
        {"name": "todate_day", "columns": dt, "source_key": "toDate(event_time)",
         "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')", "expect_ok": True},
        # toYYYYMM -> month: different days of the same month map to a single month partition.
        {"name": "toyyyymm_month", "columns": dt, "source_key": "toYYYYMM(event_time)",
         "dest_key": "toMonthNumSinceEpoch(event_time)",
         "rows": "(1, '2024-03-01 00:00:00'), (2, '2024-03-20 00:00:00')", "expect_ok": True},
        # toStartOfHour -> hour.
        {"name": "startofhour_hour", "columns": dt, "source_key": "toStartOfHour(event_time)",
         "dest_key": "toRelativeHourNum(event_time)",
         "rows": "(1, '2024-03-05 12:00:00'), (2, '2024-03-05 12:59:00')", "expect_ok": True},
        # Finer source (day + country) into a day-partitioned destination: extra column allowed.
        {"name": "finer_day", "columns": "id Int64, event_time DateTime, country String",
         "source_key": "(toDate(event_time), country)", "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00', 'US'), (2, '2024-03-05 20:00:00', 'US')",
         "expect_ok": True},
        # Compound field order reversed: matching is by column; the destination defines tuple order.
        {"name": "reversed_order", "columns": yr, "source_key": "(year, region)",
         "dest_key": "(region, year)", "rows": "(1, 2020, 'EU')", "expect_ok": True,
         "verify": [("region", "region"), ("year", "year")]},
        # Superset source: (year, region) into a year-only destination is finer, so accepted.
        {"name": "superset", "columns": yr, "source_key": "(year, region)", "dest_key": "year",
         "rows": "(1, 2020, 'EU')", "expect_ok": True, "verify": [("year", "year")]},
        # Coarser source: a month partition spans several days, so it cannot map to one day.
        {"name": "coarser_day", "columns": dt, "source_key": "toYYYYMM(event_time)",
         "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-01 00:00:00'), (2, '2024-03-20 00:00:00')", "expect_ok": False},
        # A hash is never monotonic, so min/max cannot prove anything about it, but an identity source key
        # pins k within the partition and a bucket of a single value is a single bucket.
        {"name": "bucket_from_identity_source", "columns": "id Int64, k Int64", "source_key": "k",
         "dest_key": "icebergBucket(8, k)", "rows": "(1, 10), (2, 10)", "expect_ok": True,
         "verify": [("k", "icebergBucket(8, k)")]},
        # The same bucket destination over a source key that does not pin k: nothing proves the rows of one
        # source partition hash into the same bucket.
        {"name": "bucket_needs_structural", "columns": "id Int64, k Int64",
         "source_key": "intDiv(k, 100)", "dest_key": "icebergBucket(8, k)",
         "rows": "(1, 10), (2, 20)", "expect_ok": False},
        # Identical expressions on a Nullable column: accepted structurally. The min/max proof refuses
        # Nullable (a NULL forms its own destination partition and the endpoints cannot rule it out),
        # so this only passes because the source already groups by exactly this transform. DateTime64(6)
        # round-trips through the Iceberg schema unchanged, which the structural type check requires.
        {"name": "nullable_exact_day", "columns": "id Int64, event_time Nullable(DateTime64(6))",
         "source_key": "toRelativeDayNum(event_time)", "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')",
         "source_settings": "allow_nullable_key = 1", "expect_ok": True},
        # Same, for identity, which is exempt from the structural type check.
        {"name": "nullable_exact_identity", "columns": "id Int64, k Nullable(Int64)",
         "source_key": "k", "dest_key": "k", "rows": "(1, 10), (2, 10)",
         "source_settings": "allow_nullable_key = 1", "expect_ok": True,
         "verify": [("k", "k")]},
        # A Nullable column without identical expressions falls to the min/max proof, which cannot see
        # NULLs, so it is rejected.
        {"name": "nullable_no_match", "columns": "id Int64, event_time Nullable(DateTime64(6))",
         "source_key": "toYYYYMM(event_time)", "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')",
         "source_settings": "allow_nullable_key = 1", "expect_ok": False},
    ]
    run_partition_compat_cases(node, cases)


def test_partition_transform_granularity_matrix(cluster):
    """
    Exercise the common ClickHouse temporal partition keys and the granularity relationships between
    the source key and the destination Iceberg transform. Acceptance is data-dependent (a source
    partition must be single-valued for every destination field), so a coarser source can still be
    accepted when a particular partition does not actually repartition. Accept cases are verified
    end-to-end (data + metadata); reject cases must throw BAD_ARGUMENTS.
    """
    node = cluster.instances["replica1"]
    dt = "id Int64, event_time DateTime"
    same_day = "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')"
    same_month = "(1, '2024-03-01 00:00:00'), (2, '2024-03-20 00:00:00')"
    same_year = "(1, '2024-03-05 00:00:00'), (2, '2024-09-10 00:00:00')"

    def case(name, source_key, dest_key, rows, expect_ok):
        return {"name": name, "columns": dt, "source_key": source_key, "dest_key": dest_key,
                "rows": rows, "expect_ok": expect_ok}

    cases = [
        # Common temporal keys at the same granularity as the destination transform.
        case("startofmonth_month", "toStartOfMonth(event_time)", "toMonthNumSinceEpoch(event_time)", same_month, True),
        case("yyyymmdd_day", "toYYYYMMDD(event_time)", "toRelativeDayNum(event_time)", same_day, True),
        case("startofday_day", "toStartOfDay(event_time)", "toRelativeDayNum(event_time)", same_day, True),
        case("toyear_year", "toYear(event_time)", "toYearNumSinceEpoch(event_time)", same_year, True),
        case("startofyear_year", "toStartOfYear(event_time)", "toYearNumSinceEpoch(event_time)", same_year, True),
        # Finer source into a coarser destination: a finer partition sits inside one coarser bucket.
        case("day_into_month", "toDate(event_time)", "toMonthNumSinceEpoch(event_time)", same_day, True),
        case("day_into_year", "toDate(event_time)", "toYearNumSinceEpoch(event_time)", same_day, True),
        case("hour_into_day", "toStartOfHour(event_time)", "toRelativeDayNum(event_time)",
             "(1, '2024-03-05 12:00:00'), (2, '2024-03-05 12:30:00')", True),
        case("month_into_year", "toYYYYMM(event_time)", "toYearNumSinceEpoch(event_time)", same_month, True),
        # Coarser source into a finer destination: the partition spans several destination buckets.
        case("year_into_month", "toYear(event_time)", "toMonthNumSinceEpoch(event_time)",
             "(1, '2020-01-15 00:00:00'), (2, '2020-06-15 00:00:00')", False),
        case("year_into_day", "toYear(event_time)", "toRelativeDayNum(event_time)",
             "(1, '2020-01-01 00:00:00'), (2, '2020-12-31 00:00:00')", False),
        # Same coarse/fine pair, but this year partition holds a single day, so it does not
        # repartition and is accepted - acceptance depends on the data, not the structure.
        case("year_into_day_single_day", "toYear(event_time)", "toRelativeDayNum(event_time)", same_day, True),
        # Weekly has no Iceberg equivalent: a week partition holding two days cannot map to one day.
        case("week_into_day", "toMonday(event_time)", "toRelativeDayNum(event_time)",
             "(1, '2024-03-05 00:00:00'), (2, '2024-03-07 00:00:00')", False),
    ]
    run_partition_compat_cases(node, cases)


def test_partition_multicolumn_subset(cluster):
    """
    Destination partition columns must be a subset of the source partition-key columns. A wide
    source whose partition key is a superset of the destination's is accepted (and its multi-column
    data plus per-field metadata verified); a destination partitioning by a column absent from the
    source partition key is rejected.
    """
    node = cluster.instances["replica1"]
    wide = "id Int64, event_time DateTime, region String, tenant Int32, v1 Float64, v2 String"

    cases = [
        # Destination partition columns {event_time, region} are a strict subset of the source's
        # {event_time, region, tenant}: accepted, with multi-column data and per-field metadata.
        {"name": "subset_ok", "columns": wide,
         "source_key": "(toDate(event_time), region, tenant)",
         "dest_key": "(toRelativeDayNum(event_time), region)",
         "rows": "(1, '2024-03-05 01:00:00', 'US', 7, 1.5, 'a'), "
                 "(2, '2024-03-05 20:00:00', 'US', 7, 2.5, 'b')",
         "expect_ok": True,
         "verify": [("event_time", "toRelativeDayNum(event_time)"), ("region", "region")]},
        # Destination partitions by 'region', which is not in the source partition key: rejected.
        {"name": "not_subset", "columns": "id Int64, event_time DateTime, region String",
         "source_key": "toDate(event_time)",
         "dest_key": "(toRelativeDayNum(event_time), region)",
         "rows": "(1, '2024-03-05 01:00:00', 'US'), (2, '2024-03-05 20:00:00', 'EU')",
         "expect_ok": False},
    ]
    run_partition_compat_cases(node, cases)


def test_export_partition_todate_source_matches_day_metadata(cluster):
    """
    End-to-end: a source partitioned by toDate(event_time) exports into a day-partitioned Iceberg
    table through the min/max refinement, and the day value written to the Iceberg metadata matches
    the exported data.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_todate_{uid}"
    iceberg_table = f"iceberg_todate_{uid}"

    make_rmt(node, mt_table, "id Int64, event_time DateTime", "toDate(event_time)",
             replica_name="replica1")
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05 01:00:00'), (2, '2024-03-05 12:00:00'), (3, '2024-03-05 23:00:00')"
    )
    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime",
                    partition_by="toRelativeDayNum(event_time)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    expected_day = int(node.query(
        f"SELECT DISTINCT toRelativeDayNum(event_time) FROM {iceberg_table}"
    ).strip())

    query_id = f"todate_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_days = {int(_partition_scalar(p, "event_time")) for p in partitions}
    assert meta_days == {expected_day}, (
        f"Metadata day {meta_days} must equal toRelativeDayNum {expected_day}."
    )


def test_export_partition_day_source_into_year_metadata(cluster):
    """
    End-to-end: a source partitioned by toDate(event_time) (finer) exports into a year-partitioned
    Iceberg destination (coarser). The value written to the Iceberg metadata is the year computed by
    the destination transform over the data, not the source day.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_day_year_{uid}"
    iceberg_table = f"iceberg_day_year_{uid}"

    make_rmt(node, mt_table, "id Int64, event_time DateTime", "toDate(event_time)",
             replica_name="replica1")
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05 01:00:00'), (2, '2024-03-05 12:00:00'), (3, '2024-03-05 23:00:00')"
    )
    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime",
                    partition_by="toYearNumSinceEpoch(event_time)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    expected_year = int(node.query(
        f"SELECT DISTINCT toYearNumSinceEpoch(event_time) FROM {iceberg_table}"
    ).strip())

    query_id = f"day_year_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_years = {int(_partition_scalar(p, "event_time")) for p in partitions}
    assert meta_years == {expected_year}, (
        f"Metadata year {meta_years} must equal toYearNumSinceEpoch {expected_year}."
    )


def test_export_partition_lossy_cast_dynamic_accept(cluster):
    """
    A lossy Int64 -> Int32 partition-column cast is accepted by the dynamic proof when the
    partition's values fit the destination type and map to a single Iceberg bucket. Source and
    destination use different truncate widths, so the field is proven via min/max rather than a
    structural match.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_lossy_{uid}"
    iceberg_table = f"iceberg_lossy_{uid}"

    make_rmt(node, mt_table, "id Int64, val Int64", "icebergTruncate(10, val)",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 100), (2, 109)")
    make_iceberg_s3(node, iceberg_table, "id Int64, val Int32",
                    partition_by="icebergTruncate(1000000, val)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")
    assert int(node.query(f"SELECT count() FROM {iceberg_table}").strip()) == 2


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
        SELECT committed_metadata_file FROM system.replicated_partition_exports
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
                FROM system.replicated_partition_exports
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


def setup_stats_tables(node, mt_table: str, iceberg_table: str):
    """Local variant of setup_tables using the wider schema with a Nullable column."""
    columns = "id Int32, name String, tag Nullable(String), year Int32"

    make_rmt(
        node, mt_table, columns, "year",
        order_by="id", replica_name="replica1",
    )
    node.query(
        f"""
        INSERT INTO {mt_table} (id, name, tag, year) VALUES
            (1, 'aaa', 'x',  2020),
            (2, 'mmm', NULL, 2020),
            (3, 'zzz', 'y',  2020),
            (4, 'kkk', 'z',  2021)
        """
    )

    make_iceberg_s3(node, iceberg_table, columns, partition_by="year")


def test_export_partition_writes_column_statistics(cluster):
    """
    Export a whole partition (EXPORT PARTITION ID '2020') that contains one NULL
    and verify that the resulting Iceberg manifest entry carries accurate per-file
    column statistics: record_count, file_size_in_bytes, column_sizes,
    null_value_counts, and lower/upper bounds.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_stats_{uid}"
    iceberg_table = f"iceberg_stats_{uid}"

    setup_stats_tables(node, mt_table, iceberg_table)

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    query_id = f"stats_partition_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table} ORDER BY id",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )

    entries = fetch_manifest_entries(node, query_id)
    assert_exported_stats(entries)


def test_export_partition_column_count_mismatch_source_more_is_rejected(cluster):
    """
    Source has 3 columns (id, year, extra), destination has 2 (id, year).
    The ALTER must be rejected synchronously with NUMBER_OF_COLUMNS_DOESNT_MATCH,
    nothing must be scheduled in system.replicated_partition_exports, and the
    Iceberg table must remain empty.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_more_{uid}"
    iceberg_table = f"iceberg_count_more_{uid}"

    make_rmt(node, mt_table, "id Int64, year Int32, extra String", "year",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH for source>dest column count, "
        f"got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.replicated_partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )


def test_export_partition_column_count_mismatch_source_fewer_is_rejected(cluster):
    """
    Source has 2 columns (id, year), destination has 3 (id, year, extra).
    Same expected synchronous rejection as the source>dest case.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_fewer_{uid}"
    iceberg_table = f"iceberg_count_fewer_{uid}"

    make_rmt(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int32, extra String",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH for source<dest column count, "
        f"got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.replicated_partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )


def test_export_partition_source_more_columns_allowed_with_ignore_extra_setting(cluster):
    """
    Source has 3 columns (id, year, extra), destination has 2 (id, year).
    With `export_merge_tree_part_schema_mismatch_mode = 'ignore_extra_source_columns_by_position'`,
    the export must succeed: the trailing `extra` source column is dropped
    (matched positionally) and only `id`/`year` land in the destination.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_{uid}"
    iceberg_table = f"iceberg_ignore_extra_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32, extra String",
             partition_by="year", replica_name="replica1")
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar'), (3, 2020, 'baz')"
    )

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int64, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH without the setting, got: {error!r}"
    )

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "strict",
        },
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH with schema_mismatch_mode='strict', got: {error!r}"
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"


def test_export_partition_column_count_mismatch_source_fewer_still_rejected_with_ignore_extra_setting(cluster):
    """
    `ignore_extra_source_columns_by_position` only relaxes the source-has-more-columns
    direction. Source has 2 columns (id, year), destination has 3 (id, year, extra):
    the destination cannot be filled from the source, so this must still be
    rejected synchronously even with the relaxed setting.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_fewer_{uid}"
    iceberg_table = f"iceberg_ignore_extra_fewer_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32", partition_by="year",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int64, year Int32, extra String",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
        },
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH for source<dest column count "
        f"even with ignore_extra_source_columns_by_position, got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.replicated_partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )


def test_export_partition_column_count_mismatch_source_fewer_reports_column_count_error_despite_name_mismatch(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_fewer_name_{uid}"
    iceberg_table = f"iceberg_count_fewer_name_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="renamed_id Int64, year Int32, extra Int32",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH to take precedence over the 'id'/'renamed_id' "
        f"name mismatch, got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.replicated_partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


def test_export_partition_column_count_mismatch_source_fewer_reports_column_count_error_despite_type_mismatch(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_fewer_type_{uid}"
    iceberg_table = f"iceberg_count_fewer_type_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id String, year Int32, extra Int32",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH to take precedence over the 'id' "
        f"type mismatch, got: {error!r}"
    )
    assert "INCOMPATIBLE_COLUMNS" not in error, (
        f"Column-count mismatch must be reported before any per-column cast check, got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.replicated_partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


def test_export_partition_key_arity_mismatch_is_rejected(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_pkey_arity_{uid}"
    iceberg_table = f"iceberg_pkey_arity_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int32, year Int32", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="(year, id)")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition key arity mismatch, got: {error!r}"
    )
    assert "partition" in error.lower(), (
        f"Expected error to mention the partition scheme mismatch, got: {error!r}"
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


def test_export_partition_ignore_extra_setting_prefix_contains_different_type_rejected_without_lossy_cast(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_lossy_reject_{uid}"
    iceberg_table = f"iceberg_ignore_extra_lossy_reject_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32, extra String", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
        },
    )
    assert "INCOMPATIBLE_COLUMNS" in error, (
        f"Expected INCOMPATIBLE_COLUMNS for the lossy cast on the kept 'id' column, got: {error!r}"
    )
    assert "lossy cast" in error, f"Expected 'lossy cast' in error, got: {error!r}"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


def test_export_partition_ignore_extra_setting_prefix_contains_different_type_succeeds_with_lossy_cast(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_lossy_ok_{uid}"
    iceberg_table = f"iceberg_ignore_extra_lossy_ok_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32, extra String", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    result = node.query(
        f"SELECT id, toTypeName(id), year FROM {iceberg_table} ORDER BY id"
    ).strip()
    assert result == "1\tInt32\t2020\n2\tInt32\t2020", f"Unexpected data:\n{result}"


def test_export_partition_ignore_extra_setting_prefix_contains_different_name(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_renamed_{uid}"
    iceberg_table = f"iceberg_ignore_extra_renamed_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int64, year Int32, extra String", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar'), (3, 2020, 'baz')")

    make_iceberg_s3(node=node, name=iceberg_table, columns="renamed_id Int64, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    result = node.query(
        f"SELECT renamed_id, year FROM {iceberg_table} ORDER BY renamed_id"
    ).strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"


def test_export_partition_ignore_extra_setting_is_noop_when_column_counts_match(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_noop_{uid}"
    iceberg_table = f"iceberg_ignore_extra_noop_{uid}"

    make_rmt(node=node, name=mt_table, columns="id Int32, year Int32", partition_by="year",
              replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"


def test_export_partition_column_count_mismatch_into_table_with_existing_data(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_seed_table = f"mt_existing_data_seed_{uid}"
    mt_table = f"mt_existing_data_{uid}"
    iceberg_table = f"iceberg_existing_data_{uid}"

    ignore_extra_settings = {
        "allow_insert_into_iceberg": 1,
        "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
    }

    make_rmt(node=node, name=mt_seed_table, columns="id Int32, year Int32, extra String",
              partition_by="year", replica_name="replica1")
    make_rmt(node=node, name=mt_table, columns="id Int32, year Int32, extra String",
              partition_by="year", replica_name="replica1")
    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(f"INSERT INTO {mt_seed_table} VALUES (100, 2020, 'x'), (101, 2021, 'y')")
    node.query(
        f"ALTER TABLE {mt_seed_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_seed_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")
    node.query(
        f"ALTER TABLE {mt_seed_table} EXPORT PARTITION ID '2021' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_seed_table, dest_table=iceberg_table,
                            partition_id="2021", expected_status="COMPLETED")

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'a'), (2, 2020, 'b')")
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 4, f"Expected 4 rows (2 pre-existing + 2 exported), got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n100\t2020\n101\t2021", (
        f"Unexpected data after exporting into a table with pre-existing rows:\n{result}"
    )


def test_export_partition_column_count_mismatch_into_partition_that_already_has_data(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_repeat_partition_{uid}"
    iceberg_table = f"iceberg_repeat_partition_{uid}"

    ignore_extra_settings = {
        "allow_insert_into_iceberg": 1,
        "export_merge_tree_part_schema_mismatch_mode": "ignore_extra_source_columns_by_position",
    }

    make_rmt(node=node, name=mt_table, columns="id Int32, year Int32, extra String",
              partition_by="year", replica_name="replica1")
    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'a'), (2, 2020, 'b')")
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count_after_first = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count_after_first == 2, (
        f"Expected 2 rows after first export, got {count_after_first}"
    )

    node.query(f"INSERT INTO {mt_table} VALUES (3, 2020, 'c'), (4, 2020, 'd')")
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={**ignore_extra_settings, "export_merge_tree_partition_force_export": 1},
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count_after_second = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count_after_second == 6, (
        f"Expected 6 rows (2 original + 2 duplicated by the forced re-export + 2 new) "
        f"after re-exporting an already-populated partition, got {count_after_second}"
    )

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n1\t2020\n2\t2020\n2\t2020\n3\t2020\n4\t2020", (
        f"Unexpected data after two exports of the same partition:\n{result}"
    )


def test_export_partition_with_renamed_destination_column(cluster):
    """
    Source has column `id`, destination has the same shape but the column is
    named `renamed_id`.  Positional matching must accept the export and the
    data must land in the destination under the new name.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_renamed_{uid}"
    iceberg_table = f"iceberg_renamed_{uid}"

    make_rmt(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")

    make_iceberg_s3(node, iceberg_table, "renamed_id Int64, year Int32",
                    partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(
        f"SELECT renamed_id, year FROM {iceberg_table} ORDER BY renamed_id"
    ).strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", (
        f"Unexpected data under renamed column:\n{result}"
    )


class RejectedPartitionExportCase(NamedTuple):
    src_columns: str
    src_partition_by: str
    dst_columns: str
    dst_partition_by: str
    insert_values: str
    error_substrings: tuple = ()


REJECTED_PARTITION_EXPORT_CASES = [
    pytest.param(
        RejectedPartitionExportCase(
            src_columns="a Int32, b Int32",
            src_partition_by="a",
            dst_columns="b Int32, a Int32",
            dst_partition_by="a",
            insert_values="(1, 1), (1, 2)",
            error_substrings=("partition key column",),
        ),
        id="same_partition_key_different_column_order_single_column",
    ),
    pytest.param(
        RejectedPartitionExportCase(
            src_columns="a Int32, b Int32, c Int32, val String",
            src_partition_by="(a, b, c)",
            dst_columns="c Int32, b Int32, a Int32, val String",
            dst_partition_by="(a, b, c)",
            insert_values="(1, 1, 1, 'x'), (1, 1, 1, 'y')",
            error_substrings=("partition key column",),
        ),
        id="same_partition_key_different_column_order_multi_column",
    ),
    pytest.param(
        RejectedPartitionExportCase(
            src_columns="a Int32, b Int32, c Int32, val String",
            src_partition_by="(a, b)",
            dst_columns="a Int32, b Int32, c Int32, val String",
            dst_partition_by="(a, b, c)",
            insert_values="(1, 2, 3, 'x')",
            error_substrings=("column 'c', which is not part of the source MergeTree partition key",),
        ),
        id="multi_column_partition_key_more_in_destination",
    ),
    pytest.param(
        RejectedPartitionExportCase(
            src_columns="other_id Int64, user_id Int64",
            src_partition_by="icebergBucket(8, user_id)",
            dst_columns="user_id Int64, other_id Int64",
            dst_partition_by="icebergBucket(8, user_id)",
            insert_values="(1, 42)",
            error_substrings=("partition key column",),
        ),
        id="transform_partition_key_different_column_order",
    ),
]


@pytest.mark.parametrize("case", REJECTED_PARTITION_EXPORT_CASES)
def test_export_partition_partition_key_mismatch_variants_are_rejected(cluster, case):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_rejected_{uid}"
    iceberg_table = f"iceberg_rejected_{uid}"

    make_rmt(node, mt_table, case.src_columns, case.src_partition_by, replica_name="replica1")
    make_iceberg_s3(node, iceberg_table, case.dst_columns, partition_by=case.dst_partition_by)

    node.query(f"INSERT INTO {mt_table} VALUES {case.insert_values}")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error}"
    for substring in case.error_substrings:
        assert substring in error, f"Expected {substring!r} in error, got: {error}"

    error_all = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error_all, f"Expected BAD_ARGUMENTS, got: {error_all}"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in destination after rejected export, got {count}"


def test_export_partition_multi_column_partition_key_success_all(cluster):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_multi_pkey_ok_all_{uid}"
    iceberg_table = f"iceberg_multi_pkey_ok_all_{uid}"

    cols = "a Int32, b Int32, c Int32, val String"
    make_rmt(node, mt_table, cols, "(a, b, c)", replica_name="replica1")
    make_iceberg_s3(node, iceberg_table, cols, partition_by="(a, b, c)")

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (4, 5, 6, 'y')")

    partition_ids = node.query(
        f"SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY partition_id"
    ).strip().split("\n")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )

    for pid in partition_ids:
        wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {iceberg_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n4\t5\t6\ty", f"Unexpected exported data:\n{result}"


def test_export_partition_with_castable_widening(cluster):
    """A lossless widening of both a data column (id Int32 -> Int64) and the
    partition column (year Int32 -> Int64) round-trips."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_widen_{uid}"
    iceberg_table = f"iceberg_widen_{uid}"

    make_rmt(node, mt_table, "id Int32, year Int32", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int64", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows in Iceberg table after export, got {count}"

    result = node.query(
        f"SELECT id, toTypeName(id), year, toTypeName(year) FROM {iceberg_table} ORDER BY id"
    ).strip()
    assert result == "1\tInt64\t2020\tInt64\n2\tInt64\t2020\tInt64", (
        f"Unexpected widened data:\n{result}"
    )


def test_export_partition_with_castable_narrowing_values_fit(cluster):
    """A lossy narrowing (id Int64 -> Int32) succeeds once the user opts in via
    export_merge_tree_part_allow_lossy_cast."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_narrow_fit_{uid}"
    iceberg_table = f"iceberg_narrow_fit_{uid}"

    make_rmt(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows in Iceberg table after export, got {count}"

    result = node.query(
        f"SELECT id, toTypeName(id), year FROM {iceberg_table} ORDER BY id"
    ).strip()
    assert result == "1\tInt32\t2020\n2\tInt32\t2020", (
        f"Unexpected narrowed data:\n{result}"
    )


def test_export_partition_lossy_cast_rejected_without_optin(cluster):
    """A lossy narrowing (id Int64 -> Int32) is rejected synchronously with
    INCOMPATIBLE_COLUMNS unless export_merge_tree_part_allow_lossy_cast is set."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_lossy_reject_{uid}"
    iceberg_table = f"iceberg_lossy_reject_{uid}"

    make_rmt(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} "
        f"SETTINGS allow_insert_into_iceberg = 1"
    )
    assert "INCOMPATIBLE_COLUMNS" in error, f"Expected INCOMPATIBLE_COLUMNS, got: {error!r}"
    assert "lossy cast" in error, f"Expected 'lossy cast' in error, got: {error!r}"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected no rows after a rejected export, got {count}"


def test_export_partition_runtime_cast_failure_propagates_async(cluster):
    """A String value that cannot be parsed as the destination Int32 passes the
    synchronous lossy-cast gate (with export_merge_tree_part_allow_lossy_cast = 1) but
    fails at runtime in the async worker with CANNOT_PARSE_TEXT. That is a deterministic
    value-conversion error on the part's immutable data — retrying the same part can
    never succeed — so it is classified as non-retryable and fails the whole task fast,
    without waiting for the absolute task timeout, leaving Iceberg empty.

    The task timeout is left at its large default, so reaching FAILED quickly proves the
    transition is driven by error classification rather than by a timeout.

    (Integer overflow is not used because the internal cast uses CastType::nonAccurate,
    which wraps rather than throwing.)
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_runtime_cast_fail_{uid}"
    iceberg_table = f"iceberg_runtime_cast_fail_{uid}"

    make_rmt(node, mt_table, "id String, year Int32", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES ('not a number', 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} "
        f"SETTINGS allow_insert_into_iceberg = 1, export_merge_tree_part_allow_lossy_cast = 1"
    )

    # The runtime parse error (CANNOT_PARSE_TEXT) is non-retryable, so the task fails fast.
    # No short timeout is set; FAILED within this window can only come from the
    # non-retryable classification, not from the (default, ~1 day) task timeout.
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "FAILED", timeout=60)

    exception_count = int(node.query(
        f"SELECT any(exception_count) FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip())
    assert exception_count > 0, (
        "Expected non-zero exception_count after a failed runtime cast"
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after failed export, got {count}"
    )


def test_export_partition_all_iceberg_types(cluster):
    """Every getIcebergType-supported type round-trips through an EXPORT PARTITION:
    scalars use narrower source types (explicit lossless widening CASTs), plus
    Array/Map/Tuple nested columns."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_all_types_{uid}"
    iceberg_table = f"iceberg_all_types_{uid}"

    # Scalar source types are strictly narrower than the destination; the export inserts
    # a positional widening CAST per column (Int8->Int16, UInt32->UInt64, ...). Nested
    # columns keep the same type on both sides.
    source_columns = (
        "i16 Int8, u16 UInt8, u32 UInt16, u64 UInt32, "
        "id Int16, big Int32, f32 Float32, f64 Float64, "
        "d Date, d32 Date32, dt DateTime, dt64 DateTime64(6), "
        "s String, uid UUID, "
        "arr Array(Int32), m Map(String, Int64), tup Tuple(a Int32, b String), "
        "year Int32"
    )
    dest_columns = (
        "i16 Int16, u16 UInt16, u32 UInt32, u64 UInt64, "
        "id Int32, big Int64, f32 Float32, f64 Float64, "
        "d Date, d32 Date32, dt DateTime, dt64 DateTime64(6), "
        "s String, uid UUID, "
        "arr Array(Int32), m Map(String, Int64), tup Tuple(a Int32, b String), "
        "year Int32"
    )

    make_rmt(node, mt_table, source_columns, "year", replica_name="replica1")
    make_iceberg_s3(node, iceberg_table, dest_columns, partition_by="year")

    node.query(
        f"""
        INSERT INTO {mt_table}
            (i16, u16, u32, u64, id, big, f32, f64, d, d32, dt, dt64, s, uid, arr, m, tup, year)
        VALUES (
            -100, 200, 50000, 4000000000,
            12345, 1000000000, 3.14, 2.718281828459045,
            '2024-01-15', '2024-01-15', '2024-01-15 12:30:45', '2024-01-15 12:30:45.123456',
            'hello iceberg', '550e8400-e29b-41d4-a716-446655440000',
            [1, 2, 3], {{'a': 10, 'b': 20}}, (7, 'seven'), 2024
        )
        """
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2024' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2024", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 1, f"Expected 1 row in Iceberg table, got {count}"

    result = node.query(
        f"""
        SELECT
            i16, u16, u32, u64, id, big,
            toString(d), toString(d32), toString(dt),
            s, toString(uid),
            arr, m['a'], m['b'], tup.a, tup.b, year
        FROM {iceberg_table}
        """
    ).strip()
    expected = "\t".join([
        "-100", "200", "50000", "4000000000",
        "12345", "1000000000",
        "2024-01-15", "2024-01-15", "2024-01-15 12:30:45.000000",
        "hello iceberg", "550e8400-e29b-41d4-a716-446655440000",
        "[1,2,3]", "10", "20", "7", "seven", "2024",
    ])
    assert result == expected, f"Unexpected round-trip data:\n{result!r}\nexpected:\n{expected!r}"

    # Floats compared with a tolerance to avoid formatting flakiness.
    floats_ok = node.query(
        f"SELECT abs(f32 - 3.14) < 1e-4 AND abs(f64 - 2.718281828459045) < 1e-12 FROM {iceberg_table}"
    ).strip()
    assert floats_ok == "1", f"Float round-trip outside tolerance: {floats_ok!r}"

    # DateTime64 sub-second component: assert the date part is preserved (exact format varies).
    ts_result = node.query(f"SELECT dt64 FROM {iceberg_table}").strip()
    assert "2024-01-15" in ts_result, f"DateTime64 date component missing: {ts_result!r}"


def test_export_partition_all_iceberg_types_lossy(cluster):
    """Lossy narrowing casts across types succeed with the opt-in flag: values that
    fit round-trip, Float64 -> Float32 loses precision, and Nullable columns carry
    both NULL and non-NULL (the latter via a lossy Nullable(Int64) -> Nullable(Int32))."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_lossy_types_{uid}"
    iceberg_table = f"iceberg_lossy_types_{uid}"

    # Each source column is wider than the destination, so the export inserts a lossy
    # narrowing CAST (allowed only because export_merge_tree_part_allow_lossy_cast=1).
    # Int8/UInt8 are not Iceberg-representable, so the narrowest integer dest is Int16.
    source_columns = (
        "big Int64, ubig UInt64, mid Int32, "
        "f Float64, dt DateTime64(6), d Date32, "
        "opt_s Nullable(String), opt_i Nullable(Int64), year Int32"
    )
    dest_columns = (
        "big Int32, ubig UInt32, mid Int16, "
        "f Float32, dt DateTime, d Date, "
        "opt_s Nullable(String), opt_i Nullable(Int32), year Int32"
    )

    make_rmt(node, mt_table, source_columns, "year", replica_name="replica1")
    make_iceberg_s3(node, iceberg_table, dest_columns, partition_by="year")

    # Values chosen to fit the destination types (the async cast wraps on overflow
    # rather than throwing, so out-of-range values would silently corrupt instead).
    # opt_s is NULL and opt_i is set, covering both nullable paths in one row.
    node.query(
        f"""
        INSERT INTO {mt_table} (big, ubig, mid, f, dt, d, opt_s, opt_i, year)
        VALUES (
            1000000, 2000000000, 30000,
            2.718281828459045, '2024-01-15 12:30:45.123456', '2024-01-15',
            NULL, 100, 2024
        )
        """
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2024' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2024", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 1, f"Expected 1 row in Iceberg table, got {count}"

    result = node.query(
        f"SELECT big, ubig, mid, toString(d), toString(dt), opt_s, opt_i, year FROM {iceberg_table}"
    ).strip()
    expected = "\t".join([
        "1000000", "2000000000", "30000",
        "2024-01-15", "2024-01-15 12:30:45.000000", "\\N", "100", "2024",
    ])
    assert result == expected, f"Unexpected lossy round-trip data:\n{result!r}\nexpected:\n{expected!r}"

    # Float64 -> Float32 stays within Float32 precision but is no longer exact.
    f_checks = node.query(
        f"SELECT abs(f - 2.718281828459045) < 1e-6, abs(f - 2.718281828459045) > 1e-9 FROM {iceberg_table}"
    ).strip()
    assert f_checks == "1\t1", f"Expected Float32 precision loss within tolerance, got: {f_checks!r}"


def _data_file_partition_records(entries):
    """Partition dicts of the non-delete data files described by manifest entries."""
    records = []
    for entry in entries:
        data_file = entry.get("data_file") or {}
        if data_file.get("content", 0) not in (0, None):
            continue
        partition = data_file.get("partition")
        if partition is not None:
            records.append(partition)
    return records


def _partition_scalar(partition, field):
    """Read a partition field value, tolerating an Avro-union ``{type: value}`` wrapper."""
    value = partition.get(field)
    if isinstance(value, dict):
        assert len(value) == 1, f"Unexpected partition union shape for {field!r}: {value!r}"
        value = next(iter(value.values()))
    return value


def assert_iceberg_partition_metadata(node, iceberg_table, uid, fields):
    """Assert every data-file partition record's field equals the single DISTINCT value of the
    corresponding expression over the exported destination data. `fields` is a list of
    (metadata_field_name, value_expr). String-normalized so integer transforms and identity
    string/int fields compare uniformly."""
    query_id = f"verify_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    for field_name, value_expr in fields:
        expected = node.query(
            f"SELECT DISTINCT toString({value_expr}) FROM {iceberg_table}"
        ).strip()
        got = {str(_partition_scalar(p, field_name)) for p in partitions}
        assert got == {expected}, (
            f"metadata field {field_name!r} = {got}, expected {{{expected!r}}}"
        )


def run_partition_compat_cases(node, cases):
    """Run partition-compatibility cases against the Iceberg export gate.

    Reject cases (``expect_ok=False``) are checked synchronously - the gate fires while scheduling,
    so the ALTER throws immediately. Accept cases are dispatched together, then awaited, then their
    data (full ordered row comparison against the exported source partition) and Iceberg partition
    metadata are verified. Each case is a dict: name, columns, source_key, dest_key, rows, expect_ok,
    and optional verify (list of (metadata_field_name, value_expr); defaults to
    [("event_time", dest_key)]) and source_settings (extra MergeTree settings)."""
    settings = {"allow_insert_into_iceberg": 1}

    def setup(case):
        uid = unique_suffix()
        mt_table = f"mt_{case['name']}_{uid}"
        iceberg_table = f"iceberg_{case['name']}_{uid}"
        make_rmt(node, mt_table, case["columns"], case["source_key"], replica_name="replica1",
                 extra_settings=case.get("source_settings", ""))
        node.query(f"INSERT INTO {mt_table} VALUES {case['rows']}")
        make_iceberg_s3(node, iceberg_table, case["columns"], partition_by=case["dest_key"])
        pid = first_partition_id(node, mt_table)
        return uid, mt_table, iceberg_table, pid

    for case in cases:
        if case["expect_ok"]:
            continue
        _uid, mt_table, iceberg_table, pid = setup(case)
        error = node.query_and_get_error(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
            settings=settings,
        )
        assert "BAD_ARGUMENTS" in error, f"{case['name']}: expected BAD_ARGUMENTS, got: {error!r}"

    dispatched = []
    for case in cases:
        if not case["expect_ok"]:
            continue
        uid, mt_table, iceberg_table, pid = setup(case)
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
            settings=settings,
        )
        dispatched.append((case, uid, mt_table, iceberg_table, pid))

    for case, uid, mt_table, iceberg_table, pid in dispatched:
        wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    for case, uid, mt_table, iceberg_table, pid in dispatched:
        # Export is a positional cast into the destination schema, so verify the destination equals
        # the source cast into the destination column types. Normalizing to the destination types
        # tolerates legitimate Iceberg type promotion (e.g. DateTime is stored as a microsecond
        # timestamp and returns as DateTime64(6)) while preserving destination precision, so a
        # spurious sub-second value would still surface as a mismatch.
        col_defs = node.query(
            f"SELECT name, type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{iceberg_table}' ORDER BY position"
        ).strip().split("\n")
        projection = ", ".join(
            f"CAST({name} AS {ctype})" for name, ctype in (c.split("\t") for c in col_defs)
        )
        src = node.query(f"SELECT {projection} FROM {mt_table} ORDER BY id")
        dst = node.query(f"SELECT {projection} FROM {iceberg_table} ORDER BY id")
        assert src == dst, f"{case['name']}: destination rows differ from source"
        fields = case.get("verify") or [("event_time", case["dest_key"])]
        assert_iceberg_partition_metadata(node, iceberg_table, f"{case['name']}_{uid}", fields)


def test_export_partition_bucket_type_change_rejected(cluster):
    """A bucket[N] partition column whose type changes (Int64 -> String) is rejected. The source
    hashLong grouping differs from the destination murmur(String) grouping, so a single source bucket
    can fan out across several destination buckets; bucket is not order-preserving, so this cannot be
    proven dynamically and must be rejected. This previously slipped through the structural fast path,
    which matched on transform name and width while ignoring the pre-transform cast."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_bucket_xform_{uid}"
    iceberg_table = f"iceberg_bucket_xform_{uid}"

    make_rmt(node, mt_table, "id Int64, key Int64", "icebergBucket(16, key)",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 42), (2, 42)")

    make_iceberg_s3(node, iceberg_table, "id Int64, key String",
                    partition_by="icebergBucket(16, key)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a type-changing bucket transform, got: {error!r}"
    )


def test_export_partition_truncate_type_change_rejected(cluster):
    """icebergTruncate with the same width but a changed column type (Int64 -> String) is rejected.
    Truncate is numeric on integers (120..129 -> 120) but byte-wise on strings ('120'..'129' stay
    distinct), so one source truncate bucket can map to several destination buckets. The structural
    fast path must not accept it on matching transform name and width; the dynamic proof rejects it
    because the endpoints do not collapse to a single destination value."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_trunc_xform_{uid}"
    iceberg_table = f"iceberg_trunc_xform_{uid}"

    # 120 and 129 are one Int64 truncate[10] bucket (120) but two distinct string truncations.
    make_rmt(node, mt_table, "id Int64, key Int64", "icebergTruncate(10, key)",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 120), (2, 129)")

    make_iceberg_s3(node, iceberg_table, "id Int64, key String",
                    partition_by="icebergTruncate(10, key)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a type-changing truncate transform, got: {error!r}"
    )


def test_export_partition_value_preserving_cast_not_order_preserving_rejected(cluster):
    """Int64 -> String keeps every value, but not their order: 2 and 29 are the endpoints of the
    source partition, yet the interior value 10 casts to a string that sorts outside them. The
    endpoints truncate to '2' while 10 truncates to '1', so the partition spans two destination
    buckets and must be rejected instead of being waved through as a lossless cast."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_cast_order_{uid}"
    iceberg_table = f"iceberg_cast_order_{uid}"

    make_rmt(node, mt_table, "id Int64, k Int64", "intDiv(k, 100)",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2), (2, 10), (3, 29)")

    make_iceberg_s3(node, iceberg_table, "id Int64, k String",
                    partition_by="icebergTruncate(1, k)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a non-order-preserving cast, got: {error!r}"
    )


def test_export_partition_order_preserving_cast_accepted(cluster):
    """The same shape as the rejected case, but with all values sharing a digit count: Int64 ->
    String is order-preserving over [20, 29], so the endpoints do bound the interior and the whole
    source partition truncates to the single destination bucket '2'."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_cast_order_ok_{uid}"
    iceberg_table = f"iceberg_cast_order_ok_{uid}"

    make_rmt(node, mt_table, "id Int64, k Int64", "intDiv(k, 100)",
             replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 20), (2, 25), (3, 29)")

    make_iceberg_s3(node, iceberg_table, "id Int64, k String",
                    partition_by="icebergTruncate(1, k)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    src = node.query(f"SELECT id, toString(k) FROM {mt_table} ORDER BY id").strip()
    dst = node.query(f"SELECT id, k FROM {iceberg_table} ORDER BY id").strip()
    assert src == dst, f"destination rows differ from source:\n{src}\n---\n{dst}"

    assert_iceberg_partition_metadata(node, iceberg_table, uid, [("k", "icebergTruncate(1, k)")])


def test_export_partition_timezone_mismatch_rejected(cluster):
    """A source partitioned by day in one timezone must not be treated as structurally identical to a
    destination day computed in another timezone. The source uses Asia/Tokyo (UTC+9) and the
    destination UTC; the exported part spans a UTC-day boundary while staying within one Tokyo day, so
    it maps to two destination partitions and must be rejected."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_tzmismatch_{uid}"
    iceberg_table = f"iceberg_tzmismatch_{uid}"

    make_rmt(node, mt_table, "id Int64, event_time DateTime('UTC')",
             "toRelativeDayNum(event_time, 'Asia/Tokyo')", replica_name="replica1")
    # Both instants are 2024-03-05 in Tokyo (UTC+9) but 2024-03-04 and 2024-03-05 in UTC.
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2024-03-04 16:00:00'), (2, '2024-03-05 10:00:00')"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime('UTC')",
                    partition_by="toRelativeDayNum(event_time)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1, "iceberg_partition_timezone": "UTC"},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a source/destination timezone mismatch, got: {error!r}"
    )


def test_export_partition_column_timezone_mismatch_rejected(cluster):
    """The same mismatch as above, but with the timezone carried by the column type instead of the
    partition expression. Both sides read `toRelativeDayNum(event_time)`, so the terms are identical and
    only the types differ - and DateTime types with different timezones compare equal, so the structural
    match must not be decided by type equality alone. The part stays within one Tokyo day while spanning
    two UTC days, so it maps to two destination partitions and must be rejected.

    `iceberg_partition_timezone` is deliberately left unset: setting it stamps a timezone onto the
    destination term, which alone makes the terms differ and hides what this test covers."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_coltz_{uid}"
    iceberg_table = f"iceberg_coltz_{uid}"

    make_rmt(node, mt_table, "id Int64, event_time DateTime('Asia/Tokyo')",
             "toRelativeDayNum(event_time)", replica_name="replica1")
    # Both literals are 2024-03-05 in Tokyo (the column's timezone) but 2024-03-04 and 2024-03-05 in UTC.
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2024-03-05 01:00:00'), (2, '2024-03-05 18:00:00')"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime('UTC')",
                    partition_by="toRelativeDayNum(event_time)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a partition-column timezone mismatch, got: {error!r}"
    )


def test_export_partition_commit_uses_exported_parts_not_new_inserts(cluster):
    """The deferred commit derives the Iceberg partition value only from the exact exported parts
    recorded in the manifest, never from parts inserted/merged into the source partition after
    scheduling. A month-partitioned source exports one day into a day-partitioned destination (a
    data-dependent acceptance); while the commit is wedged, an earlier day is inserted and merged in,
    so the only active part now spans both days with its min at the new day. The commit must still
    stamp the exported day (the exported part is found among Outdated parts by name), not the merged-in
    earlier day, so the metadata matches the exported data files."""
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_commit_parts_{uid}"
    iceberg_table = f"iceberg_commit_parts_{uid}"

    make_rmt(node, mt_table, "id Int64, event_date Date", "toYYYYMM(event_date)", replica_name="replica1")
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
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_days = {int(_partition_scalar(p, "event_date")) for p in partitions}
    assert meta_days == {exported_day}, (
        f"Metadata day {meta_days} must equal the exported day {exported_day} (2024-03-20), "
        f"not the injected day {injected_day} (2024-03-05)."
    )

    assert int(node.query(f"SELECT count() FROM {iceberg_table}").strip()) == 2, (
        "Only the two exported rows must be present in the destination."
    )


def test_export_partition_month_transform_metadata_matches_data(cluster):
    """A month-transform partition records a months-since-epoch value in metadata that
    matches the value derived from the exported data, and a transform-filtered read
    returns the rows."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_month_xform_{uid}"
    iceberg_table = f"iceberg_month_xform_{uid}"

    make_rmt(node, mt_table, "id Int64, event_date Date",
             "toMonthNumSinceEpoch(event_date)", replica_name="replica1")
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05'), (2, '2024-03-20'), (3, '2024-03-31')"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_date Date",
                    partition_by="toMonthNumSinceEpoch(event_date)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    month_num = int(node.query(
        f"SELECT DISTINCT toMonthNumSinceEpoch(event_date) FROM {iceberg_table}"
    ).strip())

    query_id = f"month_xform_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_values = {int(_partition_scalar(p, "event_date")) for p in partitions}
    assert meta_values == {month_num}, (
        f"Metadata month {meta_values} must equal toMonthNumSinceEpoch over the data "
        f"({month_num})."
    )

    filtered = int(node.query(
        f"SELECT count() FROM {iceberg_table} "
        f"WHERE toMonthNumSinceEpoch(event_date) = {month_num}"
    ).strip())
    assert filtered == 3, f"Transform-filtered read expected 3 rows, got {filtered}"


def test_export_partition_identity_type_change_metadata_matches_data(cluster):
    """An identity partition column whose type changes UInt16 -> String records the
    destination String value in the Iceberg metadata, matching the exported data."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_identity_xform_{uid}"
    iceberg_table = f"iceberg_identity_xform_{uid}"

    make_rmt(node, mt_table, "id Int32, year UInt16", "year", replica_name="replica1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2024), (2, 2024)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year String", partition_by="year")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows after export, got {count}"

    data_year = node.query(f"SELECT DISTINCT year FROM {iceberg_table}").strip()
    assert data_year == "2024", f"Expected exported year '2024' (String), got {data_year!r}"

    query_id = f"identity_xform_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_values = {str(_partition_scalar(p, "year")) for p in partitions}
    assert meta_values == {"2024"}, (
        f"Metadata partition {meta_values} must equal the destination String value "
        f"'2024' (not the source integer representation)."
    )


def test_export_partition_multicolumn_identity_metadata_matches_data(cluster):
    """A multi-column identity partition (event_date Date, retention UInt64 -> Int64)
    records per-column values in the Iceberg metadata that match the exported data."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_multicol_{uid}"
    iceberg_table = f"iceberg_multicol_{uid}"

    # Iceberg has no unsigned types, so retention widens UInt64 -> Int64; the cast is
    # not value-preserving per canBeSafelyCast, hence the lossy opt-in below.
    make_rmt(node, mt_table, "id Int64, event_date Date, retention UInt64",
             "(event_date, retention)", replica_name="replica1")
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05', 30), (2, '2024-03-05', 30), (3, '2024-03-05', 30)"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_date Date, retention Int64",
                    partition_by="(event_date, retention)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    data_retention = int(node.query(
        f"SELECT DISTINCT retention FROM {iceberg_table}"
    ).strip())
    assert data_retention == 30, f"Expected exported retention 30, got {data_retention}"

    days = int(node.query(
        f"SELECT DISTINCT toInt64(event_date) FROM {iceberg_table}"
    ).strip())

    query_id = f"multicol_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = _data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"

    meta_dates = {int(_partition_scalar(p, "event_date")) for p in partitions}
    assert meta_dates == {days}, (
        f"Metadata event_date {meta_dates} must equal days-since-epoch {days}."
    )
    meta_retentions = {int(_partition_scalar(p, "retention")) for p in partitions}
    assert meta_retentions == {30}, (
        f"Metadata retention {meta_retentions} must equal the exported value 30."
    )

    filtered = int(node.query(
        f"SELECT count() FROM {iceberg_table} "
        f"WHERE event_date = '2024-03-05' AND retention = 30"
    ).strip())
    assert filtered == 3, f"Partition-filtered read expected 3 rows, got {filtered}"
