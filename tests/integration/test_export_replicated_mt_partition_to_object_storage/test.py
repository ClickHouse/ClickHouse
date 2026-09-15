import logging
import time
import uuid
from typing import NamedTuple

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import (
    first_partition_id,
    make_rmt,
    wait_for_exception_count,
    wait_for_export_status,
    wait_for_export_to_start,
)
from helpers.network import PartitionManager


EXTRA_SOURCE_COLUMN_MODES = [
    pytest.param("POSITION", id="by-position"),
    pytest.param("NAME", id="by-name"),
]



def skip_if_remote_database_disk_enabled(cluster):
    """Skip test if any instance in the cluster has remote database disk enabled.

    Tests that block MinIO cannot run when remote database disk is enabled,
    as the database metadata is stored on MinIO and blocking it would break the database.
    """
    for instance in cluster.instances.values():
        if instance.with_remote_database_disk:
            pytest.skip("Test cannot run with remote database disk enabled (db disk), as it blocks MinIO which stores database metadata")


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "replica1", 
            main_configs=["configs/named_collections.xml", "configs/allow_experimental_export_partition.xml"],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )
        cluster.add_instance(
            "replica2", 
            main_configs=["configs/named_collections.xml", "configs/allow_experimental_export_partition.xml"],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )
        # node that does not participate in the export, but will have visibility over the s3 table
        cluster.add_instance(
            "watcher_node", 
            main_configs=["configs/named_collections.xml"],
            user_configs=[],
            with_minio=True,
        )
        cluster.add_instance(
            "replica_with_export_disabled", 
            main_configs=["configs/named_collections.xml", "configs/disable_experimental_export_partition.xml"],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )
        # Sharded instances for filename pattern tests
        cluster.add_instance(
            "shard1_replica1",
            main_configs=["configs/named_collections.xml", "configs/allow_experimental_export_partition.xml", "configs/macros_shard1_replica1.xml"],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["multi_read"],
        )

        cluster.add_instance(
            "shard2_replica1",
            main_configs=["configs/named_collections.xml", "configs/allow_experimental_export_partition.xml", "configs/macros_shard2_replica1.xml"],
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
    running ZooKeeper background threads (merge selector, queue log, cleanup, export manifest
    updater).  With many tables alive simultaneously the ZooKeeper session becomes overwhelmed
    and subsequent tests start seeing operation-timeout / session-expired errors.
    """
    yield
    for instance_name, instance in cluster.instances.items():
        try:
            tables_str = instance.query(
                "SELECT name FROM system.tables WHERE database = 'default' FORMAT TabSeparated"
            ).strip()
            if not tables_str:
                continue
            for table in tables_str.split('\n'):
                table = table.strip()
                if table:
                    instance.query(f"DROP TABLE IF EXISTS default.`{table}` SYNC")
        except Exception as e:
            logging.warning(f"drop_tables_after_test: cleanup failed on {instance_name}: {e}")


def create_s3_table(node, s3_table):
    node.query(f"CREATE TABLE {s3_table} (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') PARTITION BY year")


def create_tables_and_insert_data(node, mt_table, s3_table, replica_name):
    node.query(f"DROP TABLE IF EXISTS {mt_table} SYNC")
    # enable_block_number_column and enable_block_offset_column are needed for patch parts support
    node.query(f"CREATE TABLE {mt_table} (id UInt64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', '{replica_name}') PARTITION BY year ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)


def create_sharded_tables_and_insert_data(node, mt_table, s3_table, replica_name):
    """Create sharded ReplicatedMergeTree table with {shard} macro in ZooKeeper path."""
    node.query(f"CREATE TABLE {mt_table} (id UInt64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{mt_table}', '{replica_name}') PARTITION BY year ORDER BY tuple()")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)


def test_restart_nodes_during_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    node2 = cluster.instances["replica2"]
    watcher_node = cluster.instances["watcher_node"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"disaster_mt_table_{postfix}"
    s3_table = f"disaster_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")
    create_tables_and_insert_data(node2, mt_table, s3_table, "replica2")
    create_s3_table(watcher_node, s3_table)

    # Block S3/MinIO requests to keep exports alive via retry mechanism
    # This allows ZooKeeper operations to proceed quickly
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        # Block responses from MinIO (source_port matches MinIO service)
        pm_rule_reject_responses_node1 = {
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses_node1)

        pm_rule_reject_responses_node2 = {
            "instance": node2,
            "destination": node2.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses_node2)

        # Block requests to MinIO (destination: MinIO, destination_port: minio_port)
        pm_rule_reject_requests_node1 = {
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests_node1)

        pm_rule_reject_requests_node2 = {
            "instance": node2,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests_node2)
        
        export_queries = f"""
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2020' TO TABLE {s3_table};
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2021' TO TABLE {s3_table};
        """

        node.query(export_queries)

        # wait for the exports to start
        wait_for_export_to_start(node, mt_table, s3_table, "2020")
        wait_for_export_to_start(node, mt_table, s3_table, "2021")

        node.stop_clickhouse(kill=True)
        node2.stop_clickhouse(kill=True)

    assert watcher_node.query(f"SELECT count() FROM {s3_table} where year = 2020") == '0\n', "Partition 2020 was written to S3 during network delay crash"

    assert watcher_node.query(f"SELECT count() FROM {s3_table} where year = 2021") == '0\n', "Partition 2021 was written to S3 during network delay crash"

    # start the nodes, they should finish the export
    node.start_clickhouse()
    node2.start_clickhouse()

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") != f'0\n', "Export of partition 2020 did not resume after crash"

    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2021") != f'0\n', "Export of partition 2021 did not resume after crash"


def test_kill_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    node2 = cluster.instances["replica2"]
    watcher_node = cluster.instances["watcher_node"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"kill_export_mt_table_{postfix}"
    s3_table = f"kill_export_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")
    create_tables_and_insert_data(node2, mt_table, s3_table, "replica2")

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
        
        # Block responses from MinIO for node2
        pm_rule_reject_responses_node2 = {
            "instance": node2,
            "destination": node2.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_responses_node2)

        # Block requests to MinIO from node2
        pm_rule_reject_requests_node2 = {
            "instance": node2,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm.add_rule(pm_rule_reject_requests_node2)
        
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

    # check system.replicated_partition_exports for the export, status should be KILLED
    assert node.query(f"SELECT status FROM system.replicated_partition_exports WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'") == 'KILLED\n', "Partition 2020 was not killed as expected"
    assert node.query(f"SELECT status FROM system.replicated_partition_exports WHERE partition_id = '2021' and source_table = '{mt_table}' and destination_table = '{s3_table}'") == 'COMPLETED\n', "Partition 2021 was not completed, this is unexpected"

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
            f"SELECT status FROM system.replicated_partition_exports"
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
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )

    assert status.strip() == "KILLED", f"Expected KILLED status, got: {status!r}"

    exception_count = node.query(
        f"""
        SELECT any(exception_count) FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )
    assert int(exception_count.strip()) > 0, "Expected non-zero exception_count in system.replicated_partition_exports"

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
        # Use the helper default (>= one manifest-updater poll cycle): system.replicated_partition_exports
        # is served from the in-memory mirror, and while the task stays PENDING the mirror only
        # picks up new exception leaves on the next poll tick (~30s) — see helper docstring.
        wait_for_exception_count(node, mt_table, s3_table, "2020", min_exception_count=1)

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # Assert the export succeeded
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == '3\n', "Export did not succeed"
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '1\n', "Export did not succeed"

    # check system.replicated_partition_exports for the export
    assert node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"

    exception_count = node.query(
        f"""
        SELECT exception_count FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )
    assert int(exception_count.strip()) >= 1, "Expected at least one exception"


def test_export_partition_retry_backoff(cluster):
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

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

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
            f"SELECT exception_count FROM system.replicated_partition_exports"
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


def test_export_partition_file_already_exists_policy(cluster):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_partition_file_already_exists_policy_mt_table_{postfix}"
    s3_table = f"export_partition_file_already_exists_policy_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    # stop merges so part names remain stable. it is important for the test.
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    # Export all parts
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
    )

    # check system.replicated_partition_exports for the export
    assert node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"

    # wait for the exports to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # plain object storage destinations surface the commit marker file path via
    # system.replicated_partition_exports.committed_marker_file
    committed_marker_file = node.query(
        f"""
        SELECT committed_marker_file FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()
    # `committed_marker_file` is the absolute key in the bucket (same convention as
    # `destination_file_paths`); it may carry the s3_conn URL's in-bucket prefix on
    # top of the table's `filename` argument, so use a "contains" check that does
    # not depend on knowing that prefix.
    assert f"{s3_table}/commit_2020_" in committed_marker_file, \
        f"Expected committed_marker_file under {s3_table}/, got: {committed_marker_file!r}"
    # Path relative to the `s3_conn` URL, derived from the absolute key without
    # assuming a particular URL prefix.
    marker_relative_path = committed_marker_file[committed_marker_file.index(f"{s3_table}/"):]
    assert node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{marker_relative_path}', format=LineAsString)"
    ) == '1\n', f"Commit marker file does not exist at {committed_marker_file!r}"

    # try to export the partition
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1"
    )

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    assert node.query(
        f"""
        SELECT count() FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          AND status = 'COMPLETED'
        """
    ) == '1\n', "Expected the export to be marked as COMPLETED"

    # overwrite policy
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1, export_merge_tree_part_file_already_exists_policy='overwrite'"
    )

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # check system.replicated_partition_exports for the export
    # ideally we would make sure the transaction id is different, but I do not have the time to do that now
    assert node.query(
        f"""
        SELECT count() FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          AND status = 'COMPLETED'
        """
    ) == '1\n', "Expected the export to be marked as COMPLETED"

    # last but not least, let's try with the error policy. FILE_ALREADY_EXISTS is a
    # non-retryable error (retrying always hits the same existing file), so the task
    # fails fast without needing a retry budget.
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1, export_merge_tree_part_file_already_exists_policy='error'",
    )

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "FAILED")

    # check system.replicated_partition_exports for the export
    assert node.query(
        f"""
        SELECT count() FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          AND status = 'FAILED'
        """
    ) == '1\n', "Expected the export to be marked as FAILED"


def export_transaction_id(node, mt_table, s3_table):
    return node.query(
        f"""
        SELECT transaction_id FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()


def wait_for_new_export_transaction(node, mt_table, s3_table, previous_transaction_id, timeout=60):
    """Wait until the export entry carries a transaction id other than *previous_transaction_id*.

    A force re-export replaces the entry. Without this wait, the COMPLETED status of the export
    being replaced can still be visible in the in-memory mirror and satisfy a status wait
    immediately, before the new export has even started.
    """
    start_time = time.time()
    last_transaction_id = None
    while time.time() - start_time < timeout:
        last_transaction_id = export_transaction_id(node, mt_table, s3_table)
        if last_transaction_id and last_transaction_id != previous_transaction_id:
            return last_transaction_id
        time.sleep(0.2)

    raise TimeoutError(
        f"Export transaction id did not change from {previous_transaction_id!r} within {timeout}s. "
        f"Last seen: {last_transaction_id!r}"
    )


def create_split_export_tables(node, mt_table, s3_table, replica_name):
    """Create a source table whose part splits into one destination file per row on export.

    `export_merge_tree_part_max_rows_per_file` is evaluated once per chunk rather than per row
    (see `MultiFileStorageObjectStorageSink::consume`), and `MergeTreeSequentialSource` emits one
    chunk per index granule, so a part can only split at granule boundaries. With the default
    granularity a small part is a single granule and never splits at all, hence
    `index_granularity = 1`. `index_granularity_bytes = 0` disables adaptive granularity, which
    would otherwise choose the granule size itself.
    """
    node.query(f"DROP TABLE IF EXISTS {mt_table} SYNC")
    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', '{replica_name}') "
        f"PARTITION BY year ORDER BY tuple() "
        f"SETTINGS index_granularity = 1, index_granularity_bytes = 0"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)


def export_partition_split_into_files(
    node, mt_table, s3_table, force=False, policy=None, previous_transaction_id=None
):
    """Export partition 2020 with one row per destination file and wait for completion.

    Only splits per row for a table built by `create_split_export_tables`.
    """
    settings = ["export_merge_tree_part_max_rows_per_file = 1"]
    if force:
        settings.append("export_merge_tree_partition_force_export = 1")
    if policy:
        settings.append(f"export_merge_tree_part_file_already_exists_policy = '{policy}'")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS {', '.join(settings)}"
    )

    if previous_transaction_id is not None:
        wait_for_new_export_transaction(node, mt_table, s3_table, previous_transaction_id)

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")


def recorded_export_paths(node, mt_table, s3_table):
    """Destination file paths recorded for the exported parts, in the order the sink wrote them.

    Mirrors the `<export-entry>/processed/<part>/paths_in_destination` data in ZooKeeper, which
    is what the commit phase turns into the partition commit marker.
    """
    paths = node.query(
        f"""
        SELECT arrayJoin(arrayFlatten(mapValues(destination_file_paths)))
        FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )
    return [path for path in paths.splitlines() if path]


def partition_commit_marker_lines(node, mt_table, s3_table):
    """Data-file paths listed inside the partition-level commit marker."""
    committed_marker_file = node.query(
        f"""
        SELECT committed_marker_file FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert f"{s3_table}/commit_2020_" in committed_marker_file, \
        f"Expected committed_marker_file under {s3_table}/, got: {committed_marker_file!r}"

    # Path relative to the `s3_conn` URL, derived from the absolute key without assuming the
    # URL's in-bucket prefix.
    marker_relative_path = committed_marker_file[committed_marker_file.index(f"{s3_table}/"):]
    lines = node.query(
        f"SELECT * FROM s3(s3_conn, filename='{marker_relative_path}', format=LineAsString)"
    )
    return [line for line in lines.splitlines() if line]


def list_partition_directory(cluster, data_path):
    """Object keys sitting next to *data_path*, split into data files and commit markers.

    The per-part commit marker is written by `MultiFileStorageObjectStorageSink::commit` in the
    same directory as the data files, named `commit_<destination file name>`.
    """
    directory = data_path.rsplit("/", 1)[0] + "/"
    object_names = sorted(
        obj.object_name
        for obj in cluster.minio_client.list_objects(
            cluster.minio_bucket, prefix=directory, recursive=True
        )
    )
    data_files = [n for n in object_names if not n.rsplit("/", 1)[-1].startswith("commit_")]
    markers = [n for n in object_names if n.rsplit("/", 1)[-1].startswith("commit_")]
    return data_files, markers


def test_export_partition_skip_policy_reports_every_split_file(cluster):
    """A `skip` re-export of an already-exported multi-file part must record every destination
    file, not just the first one.

    The recorded list is what the commit phase turns into the partition commit marker, so
    dropping the later split files from it misrepresents the export even though the data is all
    there.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"skip_reports_all_files_mt_table_{postfix}"
    s3_table = f"skip_reports_all_files_s3_table_{postfix}"

    create_split_export_tables(node, mt_table, s3_table, "replica1")
    # The destination file name is derived from the part name, so part names have to stay stable
    # across the two exports, otherwise the second one writes to fresh paths and skips nothing.
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    export_partition_split_into_files(node, mt_table, s3_table)
    first_transaction_id = export_transaction_id(node, mt_table, s3_table)

    exported_paths = recorded_export_paths(node, mt_table, s3_table)
    assert len(exported_paths) == 3, \
        f"Expected the 3-row partition to split into 3 files, got {exported_paths}"
    assert len(partition_commit_marker_lines(node, mt_table, s3_table)) == 3

    # Re-export. Every destination file is already there, so `skip` short-circuits the part --
    # but it must do so with the complete file list.
    export_partition_split_into_files(
        node, mt_table, s3_table, force=True, policy="skip",
        previous_transaction_id=first_transaction_id,
    )

    skipped_paths = recorded_export_paths(node, mt_table, s3_table)
    assert sorted(skipped_paths) == sorted(exported_paths), (
        f"Skipped re-export recorded {skipped_paths} instead of all 3 split files {exported_paths}"
    )

    committed = partition_commit_marker_lines(node, mt_table, s3_table)
    assert len(committed) == 3, \
        f"Skipped re-export committed {len(committed)} path(s) instead of all 3 split files: {committed}"


def test_export_partition_skip_policy_reexports_incomplete_part(cluster):
    """A part whose multi-file export was interrupted must be re-exported in full under `skip`.

    The first split file existing proves nothing on its own: only the per-part commit marker,
    written after the last file is finalized, proves the part was fully exported. Removing the
    trailing files together with the marker reproduces what an attempt that died mid-part leaves
    behind, and the retry has to rewrite them -- the rows in those files are produced by no other
    attempt.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"skip_reexports_partial_mt_table_{postfix}"
    s3_table = f"skip_reexports_partial_s3_table_{postfix}"

    create_split_export_tables(node, mt_table, s3_table, "replica1")
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    export_partition_split_into_files(node, mt_table, s3_table)
    first_transaction_id = export_transaction_id(node, mt_table, s3_table)

    written_in_order = recorded_export_paths(node, mt_table, s3_table)
    assert len(written_in_order) == 3, \
        f"Expected the 3-row partition to split into 3 files, got {written_in_order}"

    data_files, markers = list_partition_directory(cluster, written_in_order[0])
    assert data_files == sorted(written_in_order), \
        f"Objects in the partition directory {data_files} do not match the recorded paths {written_in_order}"
    assert len(markers) == 1, f"Expected one per-part commit marker, got {markers}"

    # Roll the destination back to "first file finalized, nothing else": drop the trailing files
    # and the marker that would otherwise prove the part complete.
    for key in written_in_order[1:] + markers:
        cluster.minio_client.remove_object(cluster.minio_bucket, key)

    surviving_data_files, surviving_markers = list_partition_directory(cluster, written_in_order[0])
    assert surviving_data_files == [written_in_order[0]], \
        f"Expected only the first split file to remain, got {surviving_data_files}"
    assert surviving_markers == [], \
        f"Expected the per-part commit marker to be gone, got {surviving_markers}"

    export_partition_split_into_files(
        node, mt_table, s3_table, force=True, policy="skip",
        previous_transaction_id=first_transaction_id,
    )

    data_files_after, markers_after = list_partition_directory(cluster, written_in_order[0])
    assert len(data_files_after) == 3, (
        f"Retry left the part partially exported: {data_files_after} "
        f"(the interrupted attempt's missing files were never rewritten)"
    )
    assert len(markers_after) == 1, \
        f"Retry did not rewrite the per-part commit marker: {markers_after}"
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "3\n", \
        "Rows from the split files the interrupted attempt never wrote are missing from the destination"
    assert len(partition_commit_marker_lines(node, mt_table, s3_table)) == 3


def test_export_partition_feature_is_disabled(cluster):
    replica_with_export_disabled = cluster.instances["replica_with_export_disabled"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_partition_feature_is_disabled_mt_table_{postfix}"
    s3_table = f"export_partition_feature_is_disabled_s3_table_{postfix}"

    create_tables_and_insert_data(replica_with_export_disabled, mt_table, s3_table, "replica1")

    error = replica_with_export_disabled.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table};")
    assert "experimental" in error, "Expected error about disabled feature"

    # make sure kill operation also throws
    error = replica_with_export_disabled.query_and_get_error(f"KILL EXPORT PARTITION WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'")
    assert "experimental" in error, "Expected error about disabled feature"


def test_export_partition_permissions(cluster):
    """Test that export partition validates permissions correctly:
    - User needs ALTER permission on source table
    - User needs INSERT permission on destination table
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"permissions_mt_table_{postfix}"
    s3_table = f"permissions_s3_table_{postfix}"

    # Create tables as default user
    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    # Create test users with specific permissions
    node.query("CREATE USER IF NOT EXISTS user_no_alter IDENTIFIED WITH no_password")
    node.query("CREATE USER IF NOT EXISTS user_no_insert IDENTIFIED WITH no_password")
    node.query("CREATE USER IF NOT EXISTS user_with_permissions IDENTIFIED WITH no_password")

    # Grant basic access to all users
    node.query(f"GRANT SELECT ON {mt_table} TO user_no_alter")
    node.query(f"GRANT SELECT ON {s3_table} TO user_no_alter")

    # user_no_insert has ALTER on source but no INSERT on destination
    node.query(f"GRANT ALTER ON {mt_table} TO user_no_insert")
    node.query(f"GRANT SELECT ON {s3_table} TO user_no_insert")

    # user_with_permissions has both ALTER and INSERT
    node.query(f"GRANT ALTER ON {mt_table} TO user_with_permissions")
    node.query(f"GRANT INSERT ON {s3_table} TO user_with_permissions")

    # Test 1: User without ALTER permission should fail
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
        user="user_no_alter"
    )

    assert "ACCESS_DENIED" in error or "Not enough privileges" in error, \
        f"Expected ACCESS_DENIED error for user without ALTER, got: {error}"

    # Test 2: User with ALTER but without INSERT permission should fail
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
        user="user_no_insert"
    )

    assert "ACCESS_DENIED" in error or "Not enough privileges" in error, \
        f"Expected ACCESS_DENIED error for user without INSERT, got: {error}"

    # Test 3: User with both ALTER and INSERT should succeed
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
        user="user_with_permissions"
    )

    # Wait for export to complete
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # Verify the export succeeded
    result = node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020")
    assert result.strip() == "3", f"Expected 3 rows exported, got: {result}"

    # Verify system table shows COMPLETED status
    status = node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
            AND destination_table = '{s3_table}'
            AND partition_id = '2020'
        """
    )
    assert status.strip() == "COMPLETED", f"Expected COMPLETED status, got: {status}"


# assert multiple exports within a single query are executed. They all share the same query id
# and previously the transaction id was the query id, which would cause problems
def test_multiple_exports_within_a_single_query(cluster):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multiple_exports_within_a_single_query_mt_table_{postfix}"
    s3_table = f"multiple_exports_within_a_single_query_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}, EXPORT PARTITION ID '2021' TO TABLE {s3_table};")

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    # assert the exports have been executed
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == '3\n', "Export did not succeed"
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2021") == '1\n', "Export did not succeed"

    # check system.replicated_partition_exports for the exports
    assert node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"

    assert node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2021'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"


def test_pending_mutations_throw_before_export_partition(cluster):
    """Test that pending mutations before export partition throw an error."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_mutations_throw_partition_mt_table_{postfix}"
    s3_table = f"pending_mutations_throw_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0")
    assert mutations.strip() != '0', "Mutation should be pending"

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )

    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error, f"Expected error about pending mutations, got: {error}"


def test_pending_mutations_skip_before_export_partition(cluster):
    """Test that pending mutations before export partition are skipped with throw_on_pending_mutations=false."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_mutations_skip_partition_mt_table_{postfix}"
    s3_table = f"pending_mutations_skip_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0")
    assert mutations.strip() != '0', "Mutation should be pending"

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=false"
    )

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "101" not in result and "102" not in result and "103" not in result, \
        "Export should contain original data before mutation"
    assert "1\n2\n3" in result, "Export should contain original data"


def test_pending_patch_parts_throw_before_export_partition(cluster):
    """Test that pending patch parts before export partition throw an error with default settings."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_patches_throw_partition_mt_table_{postfix}"
    s3_table = f"pending_patches_throw_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    node.query(f"DROP TABLE {mt_table}")

    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error or "pending patch parts" in error.lower(), \
        f"Expected error about pending patch parts, got: {error}"


def test_pending_patch_parts_skip_before_export_partition(cluster):
    """Test that pending patch parts before export partition are skipped with throw_on_pending_patch_parts=false."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_patches_skip_partition_mt_table_{postfix}"
    s3_table = f"pending_patches_skip_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_patch_parts=false"
    )

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "1\n2\n3" in result, "Export should contain original data before patch"

    node.query(f"DROP TABLE {mt_table}")


def test_mutations_after_export_partition_started(cluster):
    """Test that mutations applied after export partition starts don't affect the exported data."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mutations_after_export_partition_mt_table_{postfix}"
    s3_table = f"mutations_after_export_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

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


def test_patch_parts_after_export_partition_started(cluster):
    """Test that patch parts created after export partition starts don't affect the exported data."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"patches_after_export_partition_mt_table_{postfix}"
    s3_table = f"patches_after_export_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

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


def test_mutation_in_partition_clause(cluster):
    """Test that mutations limited to specific partitions using IN PARTITION clause
    allow exports of unaffected partitions to succeed."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mutation_in_partition_clause_mt_table_{postfix}"
    s3_table = f"mutation_in_partition_clause_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    # Issue a mutation that uses IN PARTITION to limit it to partition 2020
    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 IN PARTITION '2020' WHERE year = 2020")

    # Verify mutation is pending for 2020
    mutations = node.query(
        f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0"
    )
    assert mutations.strip() != '0', "Mutation should be pending"

    # Export of 2020 should fail (it has pending mutations)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )
    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error, f"Expected error about pending mutations for partition 2020, got: {error}"

    # Export of 2021 should succeed (no mutations affecting it)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2021' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )

    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2021 ORDER BY id")
    assert "4" in result, "Export of partition 2021 should contain original data"


def test_export_partition_with_mixed_computed_columns(cluster):
    """Test export partition with ALIAS, MATERIALIZED, and EPHEMERAL columns."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mixed_computed_mt_table_{postfix}"
    s3_table = f"mixed_computed_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (
            id UInt32,
            value UInt32,
            tag_input String EPHEMERAL,
            doubled UInt64 ALIAS value * 2,
            tripled UInt64 MATERIALIZED value * 3,
            tag String DEFAULT upper(tag_input)
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')
        PARTITION BY id
        ORDER BY id
        SETTINGS index_granularity = 1
    """)

    # Create S3 destination table with regular columns (no EPHEMERAL)
    node.query(f"""
        CREATE TABLE {s3_table} (
            id UInt32,
            value UInt32,
            doubled UInt64,
            tripled UInt64,
            tag String
        ) ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY id
    """)

    node.query(f"INSERT INTO {mt_table} (id, value, tag_input) VALUES (1, 5, 'test'), (1, 10, 'prod')")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '1' TO TABLE {s3_table}")

    wait_for_export_status(node, mt_table, s3_table, "1", "COMPLETED")

    # Verify source data (ALIAS computed, EPHEMERAL not stored)
    source_result = node.query(f"SELECT id, value, doubled, tripled, tag FROM {mt_table} ORDER BY value")
    expected = "1\t5\t10\t15\tTEST\n1\t10\t20\t30\tPROD\n"
    assert source_result == expected, f"Source table data mismatch. Expected:\n{expected}\nGot:\n{source_result}"

    dest_result = node.query(f"SELECT id, value, doubled, tripled, tag FROM {s3_table} ORDER BY value")
    assert dest_result == expected, f"Exported data mismatch. Expected:\n{expected}\nGot:\n{dest_result}"

    status = node.query(f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
            AND destination_table = '{s3_table}'
            AND partition_id = '1'
    """)
    assert status.strip() == "COMPLETED", f"Expected COMPLETED status, got: {status}"


def test_sharded_export_partition_with_filename_pattern(cluster):
    """Test that export partition with filename pattern prevents collisions in sharded setup."""
    shard1_r1 = cluster.instances["shard1_replica1"]
    shard2_r1 = cluster.instances["shard2_replica1"]
    watcher_node = cluster.instances["watcher_node"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"sharded_mt_table_{postfix}"
    s3_table = f"sharded_s3_table_{postfix}"

    # Create sharded tables on all shards with same partition data (same part names)
    # Each shard uses different ZooKeeper path via {shard} macro
    create_sharded_tables_and_insert_data(shard1_r1, mt_table, s3_table, "replica1")
    create_sharded_tables_and_insert_data(shard2_r1, mt_table, s3_table, "replica1")
    create_s3_table(watcher_node, s3_table)

    # Export partition from both shards with filename pattern including shard
    # This should prevent filename collisions
    shard1_r1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_filename_pattern = '{{part_name}}_{{shard}}_{{replica}}_{{checksum}}'"
    )
    shard2_r1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_filename_pattern = '{{part_name}}_{{shard}}_{{replica}}_{{checksum}}'"
    )

    # Wait for exports to complete
    wait_for_export_status(shard1_r1, mt_table, s3_table, "2020", "COMPLETED")
    wait_for_export_status(shard2_r1, mt_table, s3_table, "2020", "COMPLETED")

    total_count = watcher_node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip()
    assert total_count == "6", f"Expected 6 total rows (3 from each shard), got {total_count}"

    # Verify filenames contain shard information (check via S3 directly)
    # Get all files from S3 - query from watcher_node since S3 is shared
    files_shard1 = watcher_node.query(
        f"SELECT _file FROM s3(s3_conn, filename='{s3_table}/**', format='One') WHERE _file LIKE '%shard1%' LIMIT 1"
    ).strip()
    files_shard2 = watcher_node.query(
        f"SELECT _file FROM s3(s3_conn, filename='{s3_table}/**', format='One') WHERE _file LIKE '%shard2%' LIMIT 1"
    ).strip()

    # Both shards should have files with their shard names
    assert "shard1" in files_shard1 or files_shard1 == "", f"Expected shard1 in filenames, got: {files_shard1}"
    assert "shard2" in files_shard2 or files_shard2 == "", f"Expected shard2 in filenames, got: {files_shard2}"


def test_export_partition_from_replicated_database_uses_db_shard_replica_macros(cluster):
    """Test that {shard} and {replica} in the filename pattern are expanded from the
    DatabaseReplicated identity, NOT from server config macros.

    replica1 has no <shard>/<replica> entries in its server config <macros> section.
    Without the fix buildDestinationFilename() leaves macro_info.shard/replica unset, so
    Macros::expand() falls through to the config-macros lookup and throws NO_ELEMENTS_IN_CONFIG.
    With the fix the DatabaseReplicated shard_name / replica_name are injected into macro_info
    before the expand call, and the pattern resolves correctly.
    """

    # The remote disk test suite sets the shard and replica macros in https://github.com/Altinity/ClickHouse/blob/bbabcaa96e8b7fe8f70ecd0bd4f76fb0f76f2166/tests/integration/helpers/cluster.py#L4356
    # When expanding the macros, the configured ones are preferred over the ones from the DatabaseReplicated definition.
    # Therefore, this test fails. It is easier to skip it than to fix it.
    skip_if_remote_database_disk_enabled(cluster)

    node = cluster.instances["replica1"]
    watcher_node = cluster.instances["watcher_node"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    db_name = f"repdb_{postfix}"
    table_name = "mt_table"
    s3_table = f"s3_dbreplicated_{postfix}"

    # These values exist only in the DatabaseReplicated definition – they are NOT
    # present anywhere in replica1's server config <macros>.
    db_shard = "db_shard_x"
    db_replica = "db_replica_y"

    node.query(
        f"CREATE DATABASE {db_name} "
        f"ENGINE = Replicated('/clickhouse/databases/{db_name}', '{db_shard}', '{db_replica}')")

    node.query(f"""
        CREATE TABLE {db_name}.{table_name}
        (id UInt64, year UInt16)
        ENGINE = ReplicatedMergeTree()
        PARTITION BY year ORDER BY tuple()""")

    node.query(f"INSERT INTO {db_name}.{table_name} VALUES (1, 2020), (2, 2020), (3, 2020)")
    # Stop merges so part names stay stable during the test.
    node.query(f"SYSTEM STOP MERGES {db_name}.{table_name}")

    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16) "
        f"ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') "
        f"PARTITION BY year")

    watcher_node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16) "
        f"ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') "
        f"PARTITION BY year")

    # Export with {shard} and {replica} in the pattern.
    # Before the fix: Macros::expand throws NO_ELEMENTS_IN_CONFIG because replica1 has
    # no <shard>/<replica> server config macros.
    # After the fix: DatabaseReplicated's shard_name/replica_name are wired into
    # macro_info before the expand call, so this succeeds and produces the right names.
    node.query(
        f"ALTER TABLE {db_name}.{table_name} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_filename_pattern = "
        f"'{{part_name}}_{{shard}}_{{replica}}_{{checksum}}'")

    # A FAILED status here almost certainly means the macro expansion threw
    # NO_ELEMENTS_IN_CONFIG (i.e. the fix is missing or broken).
    wait_for_export_status(node, table_name, s3_table, "2020", "COMPLETED")

    # Data should have landed in S3.
    count = watcher_node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip()
    assert count == "3", f"Expected 3 exported rows, got {count}"

    # The exported filename must contain the exact shard and replica names from the
    # DatabaseReplicated definition, proving the fix injected them (not server config macros).
    filename = watcher_node.query(
        f"SELECT _file FROM s3(s3_conn, filename='{s3_table}/**/*.parquet', format='One') LIMIT 1"
    ).strip()

    assert db_shard in filename, (
        f"Expected filename to contain DatabaseReplicated shard '{db_shard}', got: {filename!r}. "
        "Suggests {shard} was not expanded from the DatabaseReplicated identity.")

    assert db_replica in filename, (
        f"Expected filename to contain DatabaseReplicated replica '{db_replica}', got: {filename!r}. "
        "Suggests {replica} was not expanded from the DatabaseReplicated identity.")


def test_sharded_export_partition_default_pattern(cluster):
    shard1_r1 = cluster.instances["shard1_replica1"]
    shard2_r1 = cluster.instances["shard2_replica1"]
    watcher_node = cluster.instances["watcher_node"]

    mt_table = "sharded_mt_table_default"
    s3_table = "sharded_s3_table_default"

    # Create sharded tables with different ZooKeeper paths per shard
    create_sharded_tables_and_insert_data(shard1_r1, mt_table, s3_table, "replica1")
    create_sharded_tables_and_insert_data(shard2_r1, mt_table, s3_table, "replica1")
    create_s3_table(watcher_node, s3_table)

    # Export with default pattern ({part_name}_{checksum}) - may cause collisions if parts have same name and the same checksum
    shard1_r1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )
    shard2_r1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    wait_for_export_status(shard1_r1, mt_table, s3_table, "2020", "COMPLETED")
    wait_for_export_status(shard2_r1, mt_table, s3_table, "2020", "COMPLETED")

    # Both exports should complete (even if there are collisions, the overwrite policy handles it)
    # S3 tables are shared, so query from watcher_node
    total_count = watcher_node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020").strip()

    # only one file with 3 rows should be present
    assert int(total_count) == 3, f"Expected 3 rows, got {total_count}"


def test_export_partition_scheduler_skipped_when_moves_stopped(cluster):
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"sched_skip_mt_{uid}"
    s3_table = f"sched_skip_s3_{uid}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    wait_for_export_to_start(node, mt_table, s3_table, "2020")

    # Wait for several scheduler cycles (each fires every 5 s).
    # If the guard is missing the scheduler would run and data would land in S3.
    time.sleep(10)

    status = node.query(
        f"SELECT status FROM system.replicated_partition_exports"
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


def test_export_partition_resumes_after_stop_moves(cluster):
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"stop_moves_before_mt_{uid}"
    s3_table = f"stop_moves_before_s3_{uid}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MOVES {mt_table}")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    wait_for_export_to_start(node, mt_table, s3_table, "2020")

    # Give the scheduler enough time to attempt (and cancel) the part task at
    # least once, exercising the lock-release code path.
    time.sleep(5)

    status = node.query(
        f"SELECT status FROM system.replicated_partition_exports"
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


def test_export_partition_resumes_after_stop_moves_during_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)

    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"stop_moves_during_mt_{uid}"
    s3_table = f"stop_moves_during_s3_{uid}"

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

        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        # Let the tasks start executing and failing against the blocked S3.
        time.sleep(2)

        node.query(f"SYSTEM STOP MOVES {mt_table}")

        # Give the cancel callback time to fire and the lock-release path to run.
        time.sleep(3)

        status = node.query(
            f"SELECT status FROM system.replicated_partition_exports"
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


def test_export_partition_all(cluster):
    """Happy path for `ALTER TABLE ... EXPORT PARTITION ALL TO TABLE ...`.

    Schedules one export task per active partition in a single ALTER, then
    verifies every partition lands in the destination S3 table.
    """
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_all_mt_{uid}"
    s3_table = f"export_all_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY year ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2021), (3, 2022)")
    create_s3_table(node, s3_table)

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")

    for partition_id in ("2020", "2021", "2022"):
        wait_for_export_status(node, mt_table, s3_table, partition_id, "COMPLETED", timeout=60)

    row_count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert row_count == 3, f"Expected 3 rows in S3 after EXPORT PARTITION ALL, got {row_count}"


def test_export_partition_partition_column_castable_type_mismatch(cluster):
    """A lossy partition-column cast (year String -> UInt16) is rejected synchronously
    when export_merge_tree_part_allow_lossy_cast is off, scheduling nothing."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pkey_cast_mismatch_partition_mt_{postfix}"
    s3_table = f"pkey_cast_mismatch_partition_s3_{postfix}"

    # Source: year String; destination: year UInt16. PARTITION BY year on
    # both sides — same AST text — to defeat the AST equivalence check.
    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year String) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1') "
        f"PARTITION BY year "
        f"ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16) "
        f"ENGINE = S3(s3_conn, filename='{s3_table}', "
        f"format=Parquet, partition_strategy='hive') "
        f"PARTITION BY year"
    )

    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2020'), (2, '2020'), (3, '2020')"
    )

    # With a String partition column the partition_id is the SipHash of the
    # value rather than the textual representation — look it up so we can
    # reference the partition explicitly in EXPORT PARTITION ID and in
    # subsequent system.replicated_partition_exports queries.
    partition_id = node.query(
        f"SELECT partition_id FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{mt_table}' "
        f"  AND active "
        f"ORDER BY name LIMIT 1"
    ).strip()
    assert partition_id, (
        "Expected one active part on the source table after INSERT; "
        "system.parts returned nothing."
    )

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{partition_id}' "
        f"TO TABLE {s3_table}"
    )
    assert "INCOMPATIBLE_COLUMNS" in error, (
        f"Expected INCOMPATIBLE_COLUMNS for a lossy partition-column cast, "
        f"got: {error!r}"
    )
    assert "requires a lossy cast" in error and "'year'" in error, (
        f"Expected the error message to report the lossy cast on column "
        f"'year', got: {error!r}"
    )

    # Nothing scheduled: no row in system.replicated_partition_exports.
    rows_in_system_view = node.query(
        f"SELECT count() FROM system.replicated_partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{s3_table}' "
        f"  AND partition_id = '{partition_id}'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.replicated_partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    # Nothing written: no parquet file under any year=*/ partition prefix.
    files_in_s3 = node.query(
        f"SELECT count() FROM s3(s3_conn, "
        f"filename='{s3_table}/year=*/*.parquet', format='One')"
    ).strip()
    assert files_in_s3 == "0", (
        f"Expected no Parquet files in S3 after a synchronously-rejected "
        f"export, found {files_in_s3}."
    )


def test_export_partition_all_failure_modes(cluster):
    """Cover the three values of `export_merge_tree_partition_all_on_error`.

    Set up an already-fully-exported source table, then re-run EXPORT PARTITION ALL
    with each failure mode and assert the documented behavior.
    """
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_all_modes_mt_{uid}"
    s3_table = f"export_all_modes_s3_{uid}"
    empty_mt = f"export_all_empty_mt_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY year ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2021), (3, 2022)")
    create_s3_table(node, s3_table)

    # First run: schedule + wait for all partitions to complete.
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")
    for partition_id in ("2020", "2021", "2022"):
        wait_for_export_status(node, mt_table, s3_table, partition_id, "COMPLETED", timeout=60)

    # Empty table: throws BAD_ARGUMENTS (no active partitions).
    node.query(
        f"CREATE TABLE {empty_mt} (id UInt64, year UInt16)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{empty_mt}', 'replica1')"
        f" PARTITION BY year ORDER BY tuple()"
    )
    error = node.query_and_get_error(
        f"ALTER TABLE {empty_mt} EXPORT PARTITION ALL TO TABLE {s3_table}"
    )
    assert "no active partitions to export" in error, (
        f"Expected 'no active partitions' error, got: {error}"
    )

    # throw_first (default): re-run aborts on the first conflicting partition.
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_partition_all_on_error = 'throw_first'"
    )
    assert "EXPORT_PARTITION_ALREADY_EXPORTED" in error, (
        f"Expected EXPORT_PARTITION_ALREADY_EXPORTED in error, got: {error}"
    )

    # collect: aggregated PARTITION_EXPORT_FAILED message lists every conflicting partition.
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_partition_all_on_error = 'collect'"
    )
    assert "PARTITION_EXPORT_FAILED" in error, (
        f"Expected PARTITION_EXPORT_FAILED in error, got: {error}"
    )
    for partition_id in ("2020", "2021", "2022"):
        assert partition_id in error, (
            f"Expected aggregated error to mention partition {partition_id}, got: {error}"
        )

    # skip_conflicts: succeeds silently because every partition conflicts and is skipped.
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_partition_all_on_error = 'skip_conflicts'"
    )


# ---- Partition-key compatibility gate (unified with the Iceberg gate) --------------------------
#
# Plain (hive) object storage writes every row of a part to the single directory computed from the
# destination PARTITION BY, so each source partition must map to exactly one destination partition.
# The gate accepts equivalent or finer source keys (e.g. a source that adds partition columns on top
# of the destination's) and rejects source partitions that would span several destination partitions
# or that do not cover the destination partition column. Hive destinations partition by bare columns
# only, so these cases exercise the column-subset and single-value paths.


def _run_subset_accept(node, source_key):
    """Export a source partitioned by *source_key* (a superset of the destination key ``year``) into a
    hive destination partitioned by ``year``, then verify the full dataset, the hive directory layout,
    and a round-trip back into MergeTree."""
    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"subset_mt_{uid}"
    s3_table = f"subset_s3_{uid}"
    roundtrip = f"subset_roundtrip_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY {source_key} ORDER BY tuple()"
    )
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, 2020, 'US'), (2, 2020, 'FR'), (3, 2021, 'US')"
    )
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')"
        f" PARTITION BY year"
    )

    partition_ids = node.query(
        f"SELECT DISTINCT partition_id FROM system.parts"
        f" WHERE database = currentDatabase() AND table = '{mt_table}' AND active"
    ).strip().split("\n")
    assert len(partition_ids) == 3, f"expected 3 source partitions, got {partition_ids}"

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")
    for pid in partition_ids:
        wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED", timeout=90)

    src = node.query(f"SELECT id, year, country FROM {mt_table} ORDER BY id")
    dst = node.query(f"SELECT id, year, country FROM {s3_table} ORDER BY id")
    assert dst == src, f"destination rows differ from source:\nsrc={src!r}\ndst={dst!r}"

    # The destination partitions by year only: rows land in the year=<value> hive directory.
    rows_2020 = node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/year=2020/*.parquet', format='Parquet')"
    ).strip()
    rows_2021 = node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/year=2021/*.parquet', format='Parquet')"
    ).strip()
    assert rows_2020 == "2", f"expected 2 rows under year=2020, got {rows_2020}"
    assert rows_2021 == "1", f"expected 1 row under year=2021, got {rows_2021}"

    node.query(
        f"CREATE TABLE {roundtrip} (id UInt64, year UInt16, country String)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{roundtrip}', 'replica1')"
        f" PARTITION BY {source_key} ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {roundtrip} SELECT * FROM {s3_table}")
    rt = node.query(f"SELECT id, year, country FROM {roundtrip} ORDER BY id")
    assert rt == src, f"round-trip rows differ from source:\nsrc={src!r}\nrt={rt!r}"


def test_export_partition_multicolumn_subset_accepted(cluster):
    """Source partitions by (year, country); destination by year only - a coarser key that is covered
    by the source key, so every source partition has a single year and maps to exactly one destination
    partition. Accepted (this was rejected as a partition-key mismatch before the plain gate was
    unified with the Iceberg one)."""
    node = cluster.instances["replica1"]
    _run_subset_accept(node, "(year, country)")


def test_export_partition_subset_reversed_order_accepted(cluster):
    """The subset match is order-independent: a source keyed by (country, year) still covers a
    destination keyed by year."""
    node = cluster.instances["replica1"]
    _run_subset_accept(node, "(country, year)")


def test_export_partition_coarser_source_rejected(cluster):
    """Source partitions monthly (toYYYYMM(dt)); destination by the raw date. A single source part
    holding two different days would map to two destination partitions, so the gate rejects the
    export synchronously with BAD_ARGUMENTS and schedules nothing."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"coarser_mt_{uid}"
    s3_table = f"coarser_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, dt Date)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY toYYYYMM(dt) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, '2024-03-05'), (2, '2024-03-20')")
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, dt Date)"
        f" ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')"
        f" PARTITION BY dt"
    )

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"

    scheduled = node.query(
        f"SELECT count() FROM system.replicated_partition_exports"
        f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
    ).strip()
    assert scheduled == "0", f"expected nothing scheduled after a synchronous reject, got {scheduled}"


def test_export_partition_dest_column_not_in_source_key_rejected(cluster):
    """Destination partitions by a column that is not part of the source partition key; the gate
    rejects the export synchronously with BAD_ARGUMENTS naming the uncovered column."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"nocover_mt_{uid}"
    s3_table = f"nocover_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY year ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'US'), (2, 2020, 'FR')")
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')"
        f" PARTITION BY country"
    )

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"
    assert "country" in error, f"expected the error to name column 'country', got: {error!r}"


def test_export_partition_column_timezone_rendered_in_destination_zone(cluster):
    """A hive partition value lives as text in the object path and is read back in the destination
    column's time zone, so the export has to spell it the way the destination would. Spelling it in the
    source's zone names a different instant and the row reads back shifted by the offset between the
    two zones. INSERT SELECT into an identical table is the reference behavior."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"tz_mt_{uid}"
    s3_export = f"tz_export_s3_{uid}"
    s3_insert = f"tz_insert_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, ts DateTime('UTC'))"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY toDate(ts) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, '2024-03-05 15:00:00')")
    for table in (s3_export, s3_insert):
        node.query(
            f"CREATE TABLE {table} (id UInt64, ts DateTime('Asia/Tokyo'))"
            f" ENGINE = S3(s3_conn, filename='{table}', format=Parquet, partition_strategy='hive')"
            f" PARTITION BY ts"
        )

    pid = first_partition_id(node, mt_table)
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_export}")
    wait_for_export_status(node, mt_table, s3_export, pid, "COMPLETED", timeout=90)

    node.query(f"INSERT INTO {s3_insert} SELECT * FROM {mt_table}")

    source_instant = node.query(f"SELECT toUnixTimestamp(ts) FROM {mt_table}").strip()
    exported_instant = node.query(f"SELECT toUnixTimestamp(ts) FROM {s3_export}").strip()
    inserted_instant = node.query(f"SELECT toUnixTimestamp(ts) FROM {s3_insert}").strip()
    assert exported_instant == source_instant, (
        f"the exported row moved in time: source {source_instant}, destination {exported_instant}"
    )
    assert inserted_instant == source_instant, (
        f"INSERT SELECT must not move it either: source {source_instant},"
        f" destination {inserted_instant}"
    )

    # 2024-03-05 15:00:00 UTC is 2024-03-06 00:00:00 in Tokyo.
    exported_directory = node.query(
        f"SELECT DISTINCT extract(_path, 'ts=[^/]*') FROM {s3_export}"
    ).strip()
    inserted_directory = node.query(
        f"SELECT DISTINCT extract(_path, 'ts=[^/]*') FROM {s3_insert}"
    ).strip()
    assert exported_directory == "ts=2024-03-06 00:00:00", (
        f"unexpected hive directory: {exported_directory!r}"
    )
    assert inserted_directory == exported_directory, (
        f"export and INSERT SELECT disagree on the partition directory:"
        f" {exported_directory!r} vs {inserted_directory!r}"
    )


def create_wildcard_destination(node, table, columns, partition_key):
    """A wildcard destination, the only partition strategy that accepts an expression as its
    partition key: the hive strategy allows storage columns only."""
    node.query(
        f"CREATE TABLE {table} ({columns})"
        f" ENGINE = S3(s3_conn, filename='{table}/{{_partition_id}}/{{_file}}.parquet',"
        f" format=Parquet, partition_strategy='wildcard')"
        f" PARTITION BY {partition_key}"
    )


def test_export_partition_dest_argument_order_rejected(cluster):
    """The destination key intDiv(x, 100) has to be validated as written. This source part holds
    x in [201, 350], which covers the destination partitions 2 and 3, so the export must be rejected.
    Reading the arguments in the reverse order would validate intDiv(100, x) instead, which is 0 at
    both endpoints and would silently write both destination partitions into one directory."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"argorder_mt_{uid}"
    s3_table = f"argorder_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, x UInt64)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY intDiv(x, 1000) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 201), (2, 350)")
    create_wildcard_destination(node, s3_table, "id UInt64, x UInt64", "intDiv(x, 100)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"


def test_export_partition_dest_finer_expression_single_partition_accepted(cluster):
    """The same shape as the rejected case, with x in [100, 150]: the whole source partition maps to
    the single destination partition 1, so it is accepted and every row lands in one directory. The
    swapped-argument reading would refuse this one, since intDiv(100, 100) != intDiv(100, 150)."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"argorder_ok_mt_{uid}"
    s3_table = f"argorder_ok_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, x UInt64)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY intDiv(x, 1000) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 100), (2, 150)")
    create_wildcard_destination(node, s3_table, "id UInt64, x UInt64", "intDiv(x, 100)")

    pid = first_partition_id(node, mt_table)
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED", timeout=90)

    # A wildcard destination cannot be read as a table, so read the objects it wrote.
    exported = f"s3(s3_conn, filename='{s3_table}/**/*.parquet', format='Parquet', structure='id UInt64, x UInt64')"
    src = node.query(f"SELECT id, x FROM {mt_table} ORDER BY id")
    dst = node.query(f"SELECT id, x FROM {exported} ORDER BY id")
    assert dst == src, f"destination rows differ from source:\nsrc={src!r}\ndst={dst!r}"

    directories = node.query(
        f"SELECT DISTINCT extract(_path, '{s3_table}/[^/]*') FROM {exported}"
    ).strip()
    assert directories == f"{s3_table}/1", f"unexpected destination directories: {directories!r}"


def test_export_partition_dest_nested_expression_accepted(cluster):
    """A destination key that wraps the source key in a coarser transform - toYYYYMM(toDate(ts)) over
    a source keyed by toDate(ts) - is a function of the source key, so every source partition sits
    inside one destination partition whatever the data is."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"nested_mt_{uid}"
    s3_table = f"nested_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, ts DateTime)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY toDate(ts) ORDER BY tuple()"
    )
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')"
    )
    create_wildcard_destination(node, s3_table, "id UInt64, ts DateTime", "toYYYYMM(toDate(ts))")

    pid = first_partition_id(node, mt_table)
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED", timeout=90)

    exported = f"s3(s3_conn, filename='{s3_table}/**/*.parquet', format='Parquet', structure='id UInt64, ts DateTime')"
    src = node.query(f"SELECT id, ts FROM {mt_table} ORDER BY id")
    dst = node.query(f"SELECT id, ts FROM {exported} ORDER BY id")
    assert dst == src, f"destination rows differ from source:\nsrc={src!r}\ndst={dst!r}"

    directories = node.query(
        f"SELECT DISTINCT extract(_path, '{s3_table}/[^/]*') FROM {exported}"
    ).strip()
    assert directories == f"{s3_table}/202403", (
        f"unexpected destination directories: {directories!r}"
    )


def test_export_partition_dest_term_over_two_columns_rejected(cluster):
    """A destination expression over two columns is only single-valued when the source key pins both.
    This source pins b but only intDiv(a, 100), so a spans [10, 90] within one source partition and
    intDiv(a + b, 100) takes both 0 and 1 there. Per-column min/max cannot bound such an expression,
    so it is rejected; a source keyed by (a, b) would be accepted, since it pins both columns."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"twocol_mt_{uid}"
    s3_table = f"twocol_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, a UInt64, b UInt64)"
        f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')"
        f" PARTITION BY (intDiv(a, 100), b) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 10, 20), (2, 90, 20)")
    create_wildcard_destination(
        node, s3_table, "id UInt64, a UInt64, b UInt64", "intDiv(a + b, 100)"
    )

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"
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
            error_substrings=(
                "column 'c', which is not part of the source MergeTree partition key",
            ),
        ),
        id="multi_column_partition_key_more_in_destination",
    ),
]


@pytest.mark.parametrize("case", REJECTED_PARTITION_EXPORT_CASES)
def test_export_partition_partition_key_mismatch_variants_are_rejected(cluster, case):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"rejected_mt_table_{postfix}"
    s3_table = f"rejected_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} ({case.src_columns})
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')
        PARTITION BY {case.src_partition_by}
        ORDER BY tuple()
    """)

    node.query(f"""
        CREATE TABLE {s3_table} ({case.dst_columns})
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY {case.dst_partition_by}
    """)

    node.query(f"INSERT INTO {mt_table} VALUES {case.insert_values}")

    partition_id = node.query(
        f"SELECT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{partition_id}' TO TABLE {s3_table}")
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error}"
    for substring in case.error_substrings:
        assert substring in error, f"Expected {substring!r} in error, got: {error}"

    error_all = node.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")
    assert "BAD_ARGUMENTS" in error_all, f"Expected BAD_ARGUMENTS, got: {error_all}"

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 0, f"Expected 0 rows in destination after rejected export, got {count}"


@pytest.mark.parametrize(
    "dst_partition_by",
    ["(a, b, c)", "(c, b, a)", "(a, b)"],
    ids=["same", "reordered", "coarser"],
)
def test_export_partition_multi_column_partition_key_success(cluster, dst_partition_by):
    """The source key pins every column the destination partitions by, so the destination may
    also name them in another order or leave some out: each destination expression is still
    single-valued over a source partition."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multi_pkey_ok_mt_table_{postfix}"
    s3_table = f"multi_pkey_ok_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')
        PARTITION BY (a, b, c)
        ORDER BY tuple()
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY {dst_partition_by}
    """)

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (1, 2, 3, 'y')")

    partition_id = node.query(
        f"SELECT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{partition_id}' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, partition_id, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {s3_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n1\t2\t3\ty", f"Unexpected exported data:\n{result}"


def test_export_partition_multi_column_partition_key_success_all(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multi_pkey_ok_all_mt_table_{postfix}"
    s3_table = f"multi_pkey_ok_all_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', 'replica1')
        PARTITION BY (a, b, c)
        ORDER BY tuple()
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY (a, b, c)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (4, 5, 6, 'y')")

    partition_ids = node.query(
        f"SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY partition_id"
    ).strip().split("\n")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")

    for pid in partition_ids:
        wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {s3_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n4\t5\t6\ty", f"Unexpected exported data:\n{result}"


@pytest.mark.parametrize("schema_match_mode", EXTRA_SOURCE_COLUMN_MODES)
def test_export_partition_schema_match_mode_honored_by_non_initiating_replica(cluster, schema_match_mode):
    replica1 = cluster.instances["replica1"]
    replica2 = cluster.instances["replica2"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"schema_mode_cross_replica_mt_{postfix}"
    s3_table = f"schema_mode_cross_replica_s3_{postfix}"

    make_rmt(node=replica1, name=mt_table, columns="id UInt64, year UInt16, extra String",
             partition_by="year", replica_name="replica1")
    make_rmt(node=replica2, name=mt_table, columns="id UInt64, year UInt16, extra String",
             partition_by="year", replica_name="replica2")
    replica1.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar'), (3, 2020, 'baz')")
    replica2.query(f"SYSTEM SYNC REPLICA {mt_table}")

    create_s3_table(node=replica1, s3_table=s3_table)
    create_s3_table(node=replica2, s3_table=s3_table)

    replica1.query(f"SYSTEM STOP MOVES {mt_table}")

    replica1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_part_schema_match_mode = '{schema_match_mode}',"
        f" export_merge_tree_part_ignore_extra_source_columns = 1"
    )

    wait_for_export_status(node=replica1, source_table=mt_table, dest_table=s3_table,
                            partition_id="2020", expected_status="COMPLETED", timeout=60)

    count = int(replica1.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 3, f"Expected 3 rows in destination table after export, got {count}"

    result = replica1.query(f"SELECT id, year FROM {s3_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"

    replica1.query(f"SYSTEM START MOVES {mt_table}")


def test_export_partition_match_by_name_honored_by_non_initiating_replica(cluster):
    replica1 = cluster.instances["replica1"]
    replica2 = cluster.instances["replica2"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"match_by_name_cross_replica_mt_{postfix}"
    s3_table = f"match_by_name_cross_replica_s3_{postfix}"

    source_columns = "id UInt64, year UInt16, omitted String, payload String"
    make_rmt(
        node=replica1,
        name=mt_table,
        columns=source_columns,
        partition_by="year",
        replica_name="replica1",
    )
    make_rmt(
        node=replica2,
        name=mt_table,
        columns=source_columns,
        partition_by="year",
        replica_name="replica2",
    )
    replica1.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, 2020, 'left', 'first'), (2, 2020, 'right', 'second')"
    )
    replica2.query(f"SYSTEM SYNC REPLICA {mt_table}")

    for replica in (replica1, replica2):
        replica.query(
            f"CREATE TABLE {s3_table} (payload String, year UInt16, id UInt64) "
            f"ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, "
            f"partition_strategy='hive') PARTITION BY year"
        )

    replica1.query(f"SYSTEM STOP MOVES {mt_table}")

    replica1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_part_schema_match_mode = 'NAME',"
        f" export_merge_tree_part_ignore_extra_source_columns = 1"
    )

    wait_for_export_status(
        node=replica1,
        source_table=mt_table,
        dest_table=s3_table,
        partition_id="2020",
        expected_status="COMPLETED",
        timeout=60,
    )

    result = replica1.query(
        f"SELECT payload, year, id FROM {s3_table} ORDER BY id"
    ).strip()
    assert result == "first\t2020\t1\nsecond\t2020\t2", f"Unexpected data:\n{result}"

    replica1.query(f"SYSTEM START MOVES {mt_table}")


def test_export_partition_match_by_name_with_equal_column_count_reordered(cluster):
    """Test that match_by_name matches columns by name even with an equal source/destination column count."""
    replica1 = cluster.instances["replica1"]
    replica2 = cluster.instances["replica2"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"match_by_name_equal_count_mt_{postfix}"
    s3_table = f"match_by_name_equal_count_s3_{postfix}"

    source_columns = "id UInt64, year UInt16, payload String"
    make_rmt(node=replica1, name=mt_table, columns=source_columns,
             partition_by="year", replica_name="replica1")
    make_rmt(node=replica2, name=mt_table, columns=source_columns,
             partition_by="year", replica_name="replica2")
    replica1.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")
    replica2.query(f"SYSTEM SYNC REPLICA {mt_table}")

    for replica in (replica1, replica2):
        replica.query(
            f"CREATE TABLE {s3_table} (payload String, id UInt64, year UInt16) "
            f"ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, "
            f"partition_strategy='hive') PARTITION BY year"
        )

    replica1.query(f"SYSTEM STOP MOVES {mt_table}")

    replica1.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_part_schema_match_mode = 'NAME'"
    )

    wait_for_export_status(node=replica1, source_table=mt_table, dest_table=s3_table,
                            partition_id="2020", expected_status="COMPLETED", timeout=60)

    result = replica1.query(f"SELECT id, year, payload FROM {s3_table} ORDER BY id").strip()
    assert result == "1\t2020\tfoo\n2\t2020\tbar", f"Unexpected data:\n{result}"

    replica1.query(f"SYSTEM START MOVES {mt_table}")
