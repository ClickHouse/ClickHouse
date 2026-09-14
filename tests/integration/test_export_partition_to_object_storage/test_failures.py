import time
import uuid

from helpers.export_partition_helpers import (
    setup_source_tables,
    skip_if_remote_database_disk_enabled,
    wait_for_exception_count,
    wait_for_export_status,
    wait_for_export_to_start,
)
from helpers.network import PartitionManager

from .common import (
    create_s3_table,
    create_tables_and_insert_data,
)

CLUSTER_INSTANCES = ["replica1", "replica2", "watcher_node"]

# `EXPORT PARTITION` under injected failure: object storage cut off with `PartitionManager`,
# failpoints, stopped moves, killed and timed-out tasks. These pace themselves against retry
# back-off and scheduler ticks, so they are slow and timing sensitive - kept out of the parallel
# batch.


def test_kill_export(cluster, source_engine):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    node2 = cluster.instances["replica2"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"kill_export_mt_table_{postfix}"
    s3_table = f"kill_export_s3_table_{postfix}"

    hosts = setup_source_tables(
        [node, node2],
        mt_table,
        "id UInt64, year UInt16",
        "year",
        source_engine,
        insert_values="(1, 2020), (2, 2020), (3, 2020), (4, 2021)",
    )
    for host in hosts:
        create_s3_table(host, s3_table)

    # Block S3/MinIO requests to keep exports alive via retry mechanism
    # This allows ZooKeeper operations (KILL) to proceed quickly
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        # Every host that can export a part has to be cut off from MinIO, otherwise it would
        # finish the export before the KILL is processed.
        for host in hosts:
            # Block responses from MinIO (source_port matches MinIO service)
            pm.add_rule({
                "instance": host,
                "destination": host.ip_address,
                "protocol": "tcp",
                "source_port": minio_port,
                "action": "REJECT --reject-with tcp-reset",
            })

            # Block requests to MinIO (destination: MinIO, destination_port: minio_port)
            pm.add_rule({
                "instance": host,
                "destination": minio_ip,
                "protocol": "tcp",
                "destination_port": minio_port,
                "action": "REJECT --reject-with tcp-reset",
            })

        export_queries = f"""
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2020' TO TABLE {s3_table};
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2021' TO TABLE {s3_table};
        """

        node.query(export_queries)
        
        # Kill only 2020 while S3 is blocked - retry mechanism keeps exports alive
        # ZooKeeper operations (KILL) proceed quickly since only S3 is blocked
        node.query(f"KILL EXPORT PARTITION WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'")

        # sleep for a while to let the kill to be processed
        time.sleep(2)

    # wait for 2021 to finish
    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    # checking for the commit file because maybe the data file was too fast?
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '0\n', "Partition 2020 was written to S3, it was not killed as expected"
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2021_*', format=LineAsString)") != f'0\n', "Partition 2021 was not written to S3, but it should have been"

    # check system.partition_exports for the export, status should be KILLED
    assert node.query(f"SELECT status FROM system.partition_exports WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'") == 'KILLED\n', "Partition 2020 was not killed as expected"
    assert node.query(f"SELECT status FROM system.partition_exports WHERE partition_id = '2021' and source_table = '{mt_table}' and destination_table = '{s3_table}'") == 'COMPLETED\n', "Partition 2021 was not completed, this is unexpected"

    # check the data did not land on s3
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == '0\n', "Partition 2020 was written to S3, it was not killed as expected"


def test_kill_export_resilient_to_status_handling_failure(cluster):
    """KILL EXPORT PARTITION must eventually take effect even when the first
    attempt to handle the ZK status-change event throws (simulated via a ONCE
    failpoint).  The re-queue + reschedule mechanism retries after ~5 s and
    the second attempt succeeds because the ONCE failpoint has already fired."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"kill_resilient_mt_{postfix}"
    s3_table = f"kill_resilient_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

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

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
        )

        node.query("SYSTEM ENABLE FAILPOINT export_partition_status_change_throw")

        node.query(
            f"KILL EXPORT PARTITION WHERE partition_id = '2020'"
            f" AND source_table = '{mt_table}' AND destination_table = '{s3_table}'")

        # sleep for a while to let the kill to be processed
        time.sleep(5)

    # The ONCE failpoint makes the first handleStatusChanges() throw.
    # The catch re-queues the key and scheduleAfter(5000) arms a retry.
    # Wait up to 15 s (5 s retry delay + margin) for the kill to propagate.
    wait_for_export_status(node, mt_table, s3_table, "2020", "KILLED", timeout=15)

    assert (
        node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE partition_id = '2020'"
            f"   AND source_table = '{mt_table}'"
            f"   AND destination_table = '{s3_table}'"
        ).strip() == "KILLED"
    ), "Export was not killed — status change was lost after the injected failure"


def test_drop_source_table_during_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    # node2 = cluster.instances["replica2"]
    watcher_node = cluster.instances["watcher_node"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"drop_source_table_during_export_mt_table_{postfix}"
    s3_table = f"drop_source_table_during_export_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")
    # create_tables_and_insert_data(node2, mt_table, s3_table, "replica2")
    create_s3_table(watcher_node, s3_table)

    # Block S3/MinIO requests to keep exports alive via retry mechanism
    # This allows ZooKeeper operations (KILL) to proceed quickly
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        # Block responses from MinIO (source_port matches MinIO service)
        pm_rule_reject_responses = {
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses)

        # Block requests to MinIO (destination: MinIO, destination_port: minio_port)
        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests)
        
        export_queries = f"""
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS s3_retry_attempts = 500;
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2021' TO TABLE {s3_table} SETTINGS s3_retry_attempts = 500;
        """

        node.query(export_queries)

        wait_for_export_status(node, mt_table, s3_table, "2020", "PENDING")
        wait_for_export_status(node, mt_table, s3_table, "2021", "PENDING")

        # This should kill the background operations and drop the table
        node.query(f"DROP TABLE {mt_table}")

    # Sleep some time to let the export finish (assuming it was not properly cancelled)
    time.sleep(10)

    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_*', format=LineAsString)") == '0\n', "Background operations completed even with the table dropped"


def test_concurrent_exports_to_different_targets(cluster):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"concurrent_diff_targets_mt_table_{postfix}"
    s3_table_a = f"concurrent_diff_targets_s3_a_{postfix}"
    s3_table_b = f"concurrent_diff_targets_s3_b_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table_a, "replica1")
    create_s3_table(node, s3_table_b)

    # Launch two exports of the same partition to two different S3 tables concurrently
    with PartitionManager() as pm:
        pm.add_network_delay(node, delay_ms=1000)

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table_a}"
        )
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table_b}"
        )

    wait_for_export_status(node, mt_table, s3_table_a, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, s3_table_b, "2020", "COMPLETED")

    # Both targets should receive the same data independently
    assert node.query(f"SELECT count() FROM {s3_table_a} WHERE year = 2020") == '3\n', "First target did not receive expected rows"
    assert node.query(f"SELECT count() FROM {s3_table_b} WHERE year = 2020") == '3\n', "Second target did not receive expected rows"

    # And both should have a commit marker
    assert node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table_a}/commit_2020_*', format=LineAsString)"
    ) != '0\n', "Commit file missing for first target"
    assert node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table_b}/commit_2020_*', format=LineAsString)"
    ) != '0\n', "Commit file missing for second target"


def test_failure_is_logged_in_system_table(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"failure_is_logged_in_system_table_mt_table_{postfix}"
    s3_table = f"failure_is_logged_in_system_table_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    # Block traffic to/from MinIO to force upload errors and retries, following existing S3 tests style
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        # Block responses from MinIO (source_port matches MinIO service)
        pm_rule_reject_responses = {
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses)

        # Also block requests to MinIO (destination: MinIO, destination_port: 9001) with REJECT to fail fast
        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests)

        # Blocked MinIO produces transient (retryable) S3 errors. There is no retry
        # budget anymore, so the task keeps retrying and is only torn down once the
        # absolute task timeout fires (transitioning to KILLED). Use a small timeout
        # so the test does not wait for the default (a day).
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
            f" SETTINGS export_merge_tree_partition_task_timeout_seconds = 5;"
        )

        # Wait for the timeout to kill the stuck task. The KILL is a Keeper operation
        # (MinIO being blocked does not affect it); the status mirror needs roughly one
        # manifest-updater poll cycle (~30s) plus watch propagation on top of the 5s
        # timeout, so allow a generous budget.
        wait_for_export_status(node, mt_table, s3_table, "2020", "KILLED", timeout=90)

    # Network restored; verify the export is marked as KILLED in the system table
    # Also verify we captured at least one exception and no commit file exists
    status = node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )

    assert status.strip() == "KILLED", f"Expected KILLED status, got: {status!r}"

    exception_count = node.query(
        f"""
        SELECT any(exception_count) FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )
    assert int(exception_count.strip()) > 0, "Expected non-zero exception_count in system.partition_exports"

    # No commit should have been produced for this partition
    assert node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)"
    ) == '0\n', "Commit file exists despite forced S3 failures"


def test_inject_short_living_failures(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"inject_short_living_failures_mt_table_{postfix}"
    s3_table = f"inject_short_living_failures_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    # Block traffic to/from MinIO to force upload errors and retries, following existing S3 tests style
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        # Block responses from MinIO (source_port matches MinIO service)
        pm_rule_reject_responses = {
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses)

        # Also block requests to MinIO (destination: MinIO, destination_port: 9001) with REJECT to fail fast
        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests)

        # Transient (retryable) failures never fail the task on a budget; it keeps
        # retrying until the network is restored and the export completes.
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table};"
        )

        # wait for at least one exception to occur, but not enough to finish the export.
        # Use the helper default (>= one manifest-updater poll cycle): system.partition_exports
        # is served from the in-memory mirror, and while the task stays PENDING the mirror only
        # picks up new exception leaves on the next poll tick (~30s) — see helper docstring.
        wait_for_exception_count(node, mt_table, s3_table, "2020", min_exception_count=1)

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # Assert the export succeeded
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == '3\n', "Export did not succeed"
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '1\n', "Export did not succeed"

    # check system.partition_exports for the export
    assert node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"

    exception_count = node.query(
        f"""
        SELECT exception_count FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )
    assert int(exception_count.strip()) >= 1, "Expected at least one exception"


def test_export_partition_retry_backoff(cluster, source_engine):
    """Verify the per-replica in-memory exponential back-off between failed part exports.

    The back-off is local in-memory state (no ZooKeeper retry_count / next_retry_time
    anymore), so it is not directly observable; instead we observe its effect. With a
    large back-off, a part that keeps failing (object storage blocked) is parked for the
    back-off window after its first failure and must NOT be retried on every ~5s
    scheduler tick. We assert that exception_count stays low across a window that spans
    several ticks. Once the network is restored and the back-off elapses, the export
    completes (there is no retry budget to exhaust)."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"retry_backoff_mt_table_{postfix}"
    s3_table = f"retry_backoff_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    # Large back-off so a single failed attempt parks the part well beyond the
    # ~5s scheduler tick. Kept moderate so the export can still complete promptly
    # once the network is restored.
    initial_backoff_seconds = 30
    max_backoff_seconds = 30

    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        # Block responses from MinIO (source_port matches MinIO service)
        pm.add_rule({
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })
        # Also block requests to MinIO to fail fast
        pm.add_rule({
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
            f"SETTINGS export_merge_tree_partition_retry_initial_backoff_seconds = {initial_backoff_seconds}, "
            f"export_merge_tree_partition_retry_max_backoff_seconds = {max_backoff_seconds}"
        )

        # Wait until the first failure is recorded.
        count_after_first = wait_for_exception_count(
            node, mt_table, s3_table, "2020", min_exception_count=1, timeout=60
        )

        # While the part is backing off (~30s) it must not be retried again. Observe
        # across a window that spans several scheduler ticks: without back-off the
        # ~5s tick would add roughly five more failures, so a small increase proves
        # the back-off is pacing retries.
        time.sleep(25)
        count_during_backoff = int(node.query(
            f"SELECT exception_count FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{s3_table}'"
            f"   AND partition_id = '2020'"
        ).strip())
        assert count_during_backoff - count_after_first <= 2, (
            f"exception_count jumped during the back-off window: "
            f"{count_after_first} -> {count_during_backoff}; back-off was not applied"
        )

    # Network restored; once the back-off elapses the export should complete because
    # there is no retry budget to exhaust.
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED", timeout=120)
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "3\n", "Export did not succeed"


def test_mutations_after_export_partition_started(cluster, source_engine):
    """Test that mutations applied after export partition starts don't affect the exported data."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mutations_after_export_partition_mt_table_{postfix}"
    s3_table = f"mutations_after_export_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    # Block traffic to MinIO to delay export
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        pm_rule_reject_responses = {
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses)

        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests)

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
            f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
        )

        # Wait for export to start
        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "1\n2\n3" in result, "Export should contain original data before mutation"
    assert "101" not in result, "Export should not contain mutated data"


def test_patch_parts_after_export_partition_started(cluster, source_engine):
    """Test that patch parts created after export partition starts don't affect the exported data."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"patches_after_export_partition_mt_table_{postfix}"
    s3_table = f"patches_after_export_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    # Block traffic to MinIO to delay export
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        pm_rule_reject_responses = {
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses)

        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests)

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
        )

        # Wait for export to start
        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "1\n2\n3" in result, "Export should contain original data before patch"
    assert "101" not in result, "Export should not contain patched data"

    node.query(f"DROP TABLE {mt_table}")


def test_export_partition_scheduler_skipped_when_moves_stopped(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"sched_skip_mt_{uid}"
    s3_table = f"sched_skip_s3_{uid}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    wait_for_export_to_start(node, mt_table, s3_table, "2020")

    # Wait for several scheduler cycles (each fires every 5 s).
    # If the guard is missing the scheduler would run and data would land in S3.
    time.sleep(10)

    status = node.query(
        f"SELECT status FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
        f" AND partition_id = '2020'"
    ).strip()

    assert status == "PENDING", (
        f"Expected PENDING while moves are stopped, got '{status}'"
    )

    row_count = int(node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip())
    assert row_count == 0, (
        f"Expected 0 rows in S3 while scheduler is skipped, got {row_count}"
    )

    node.query(f"SYSTEM START MOVES {mt_table}")

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED", timeout=60)

    row_count = int(node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip())
    assert row_count == 3, f"Expected 3 rows in S3 after export completed, got {row_count}"


def test_export_partition_resumes_after_stop_moves(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"stop_moves_before_mt_{uid}"
    s3_table = f"stop_moves_before_s3_{uid}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    wait_for_export_to_start(node, mt_table, s3_table, "2020")

    # Give the scheduler enough time to attempt (and cancel) the part task at
    # least once, exercising the lock-release code path.
    time.sleep(5)

    status = node.query(
        f"SELECT status FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
        f" AND partition_id = '2020'"
    ).strip()
    assert status == "PENDING", f"Expected PENDING while moves are stopped, got '{status}'"

    row_count = int(node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip())
    assert row_count == 0, f"Expected 0 rows in S3 while moves are stopped, got {row_count}"

    node.query(f"SYSTEM START MOVES {mt_table}")

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED", timeout=60)

    row_count = int(node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip())
    assert row_count == 3, f"Expected 3 rows in S3 after export completed, got {row_count}"


def test_export_partition_resumes_after_stop_moves_during_export(cluster, source_engine):
    skip_if_remote_database_disk_enabled(cluster)

    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"stop_moves_during_mt_{uid}"
    s3_table = f"stop_moves_during_s3_{uid}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

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

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
        )

        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        # Let the tasks start executing and failing against the blocked S3.
        time.sleep(2)

        node.query(f"SYSTEM STOP MOVES {mt_table}")

        # Give the cancel callback time to fire and the lock-release path to run.
        time.sleep(3)

        status = node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
            f" AND partition_id = '2020'"
        ).strip()

        assert status == "PENDING", (
            f"Expected PENDING while moves are stopped and S3 is blocked, got '{status}'"
        )

        node.query(f"SYSTEM START MOVES {mt_table}")

    # MinIO is now unblocked; the next scheduler cycle should succeed.
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED", timeout=60)

    row_count = int(node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip())
    assert row_count == 3, f"Expected 3 rows in S3 after export completed, got {row_count}"


# ---- Dispatch-time destination validation ------------------------------------------------------

def test_dispatch_fails_when_destination_dropped(cluster):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"dispatch_drop_mt_{postfix}"
    s3_table = f"dispatch_drop_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine="MergeTree")

    node.query(f"SYSTEM STOP MOVES {mt_table}")
    try:
        node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        status = node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
            f" AND partition_id = '2020'"
        ).strip()
        assert status == "PENDING", f"Expected PENDING while moves are stopped, got {status!r}"

        node.query(f"DROP TABLE {s3_table} SYNC")
        node.query(f"SYSTEM START MOVES {mt_table}")

        wait_for_export_status(node, mt_table, s3_table, "2020", "FAILED", timeout=60)

        last_exceptions = node.query(
            f"SELECT last_exception_per_replica FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{s3_table}'"
            f"   AND partition_id = '2020'"
        ).strip()
        assert last_exceptions not in ("", "[]"), (
            "Expected an exception to be recorded for the dropped destination"
        )
    finally:
        node.query(f"SYSTEM START MOVES {mt_table}")


def test_dispatch_fails_when_source_part_detached(cluster):
    """A source part that disappears after schedule cannot be restored on a single-node
    `MergeTree`, so the task must go to FAILED immediately rather than retrying until timeout.

    `DETACH PARTITION` (not `DROP`) is required: the scheduler pins `DataPartPtr`s, which keep a
    dropped part in `Outdated` and still findable. Detach removes it from the parts index, which
    is what `exportPartToTable` looks up.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"dispatch_detach_mt_{postfix}"
    s3_table = f"dispatch_detach_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine="MergeTree")

    node.query(f"SYSTEM STOP MOVES {mt_table}")
    try:
        node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        status = node.query(
            f"SELECT status FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
            f" AND partition_id = '2020'"
        ).strip()
        assert status == "PENDING", f"Expected PENDING while moves are stopped, got {status!r}"

        node.query(f"ALTER TABLE {mt_table} DETACH PARTITION ID '2020'")
        node.query(f"SYSTEM START MOVES {mt_table}")

        wait_for_export_status(node, mt_table, s3_table, "2020", "FAILED", timeout=60)

        last_exceptions = node.query(
            f"SELECT last_exception_per_replica FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{s3_table}'"
            f"   AND partition_id = '2020'"
        ).strip()
        assert last_exceptions not in ("", "[]"), (
            "Expected an exception to be recorded for the missing source part"
        )
        assert "No such data part" in last_exceptions, (
            f"Expected NO_SUCH_DATA_PART in last_exception, got {last_exceptions!r}"
        )
    finally:
        node.query(f"SYSTEM START MOVES {mt_table}")


def test_dispatch_fails_when_destination_schema_incompatible(cluster):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"dispatch_schema_mt_{postfix}"
    s3_table = f"dispatch_schema_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine="MergeTree")

    node.query(f"SYSTEM STOP MOVES {mt_table}")
    try:
        node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        node.query(f"DROP TABLE {s3_table} SYNC")
        # An extra destination column is always rejected (the source has only id and year).
        node.query(
            f"CREATE TABLE {s3_table} (id UInt64, year UInt16, extra String) "
            f"ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') "
            f"PARTITION BY year"
        )
        node.query(f"SYSTEM START MOVES {mt_table}")

        wait_for_export_status(node, mt_table, s3_table, "2020", "FAILED", timeout=60)

        last_exceptions = node.query(
            f"SELECT last_exception_per_replica FROM system.partition_exports"
            f" WHERE source_table = '{mt_table}'"
            f"   AND destination_table = '{s3_table}'"
            f"   AND partition_id = '2020'"
        ).strip()
        assert last_exceptions not in ("", "[]"), (
            "Expected an exception to be recorded for the schema mismatch"
        )
    finally:
        node.query(f"SYSTEM START MOVES {mt_table}")


def test_export_task_timeout_kills_stuck_pending_task(cluster, source_engine):
    """A task that cannot make progress is torn down by the absolute task timeout, and the reason
    is reported through the system table rather than being silently dropped."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"timeout_mt_{postfix}"
    s3_table = f"timeout_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

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

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
            f" SETTINGS export_merge_tree_partition_task_timeout_seconds = 5"
        )

        wait_for_export_status(node, mt_table, s3_table, "2020", "KILLED", timeout=90)

    last_exceptions = node.query(
        f"SELECT last_exception_per_replica FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}'"
        f"   AND destination_table = '{s3_table}'"
        f"   AND partition_id = '2020'"
    ).strip()
    assert "timed out" in last_exceptions, (
        f"Expected the recorded exception to mention the timeout reason, got: {last_exceptions!r}"
    )

    assert node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)"
    ) == "0\n", "Commit file exists despite the task timeout"
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "0\n"
