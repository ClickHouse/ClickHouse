import logging
import pytest
import random
import string
import time
from typing import Optional
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


def wait_for_export_status(
    node,
    mt_table: str,
    s3_table: str,
    partition_id: str,
    expected_status: str = "COMPLETED",
    timeout: int = 30,
    poll_interval: float = 0.5,
):
    start_time = time.time()
    last_status = None
    while time.time() - start_time < timeout:
        status = node.query(
            f"""
            SELECT status FROM system.replicated_partition_exports
            WHERE source_table = '{mt_table}'
                AND destination_table = '{s3_table}'
                AND partition_id = '{partition_id}'
            """
        ).strip()
        
        last_status = status

        if status and status == expected_status:
            return status

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Export status did not reach '{expected_status}' within {timeout}s. Last status: '{last_status}'")


def wait_for_export_to_start(
    node,
    mt_table: str,
    s3_table: str,
    partition_id: str,
    timeout: int = 10,
    poll_interval: float = 0.2,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        count = node.query(
            f"""
            SELECT count() FROM system.replicated_partition_exports
            WHERE source_table = '{mt_table}'
              AND destination_table = '{s3_table}'
              AND partition_id = '{partition_id}'
            """
        ).strip()
        
        if count != '0':
            return True
        
        time.sleep(poll_interval)
    
    raise TimeoutError(f"Export did not start within {timeout}s. ")


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
        )
        cluster.add_instance(
            "replica2", 
            main_configs=["configs/named_collections.xml", "configs/allow_experimental_export_partition.xml"],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
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
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_s3_table(node, s3_table):
    node.query(f"CREATE TABLE {s3_table} (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') PARTITION BY year")


def create_tables_and_insert_data(node, mt_table, s3_table, replica_name):
    node.query(f"CREATE TABLE {mt_table} (id UInt64, year UInt16) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{mt_table}', '{replica_name}') PARTITION BY year ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)


def test_restart_nodes_during_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    node2 = cluster.instances["replica2"]
    watcher_node = cluster.instances["watcher_node"]

    mt_table = "disaster_mt_table"
    s3_table = "disaster_s3_table"

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
            EXPORT PARTITION ID '2020' TO TABLE {s3_table}
            SETTINGS export_merge_tree_partition_max_retries = 50;
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2021' TO TABLE {s3_table}
            SETTINGS export_merge_tree_partition_max_retries = 50;
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

    mt_table = "kill_export_mt_table"
    s3_table = "kill_export_s3_table"

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
            EXPORT PARTITION ID '2020' TO TABLE {s3_table}
            SETTINGS export_merge_tree_partition_max_retries = 50;
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2021' TO TABLE {s3_table}
            SETTINGS export_merge_tree_partition_max_retries = 50;
        """

        node.query(export_queries)
        
        # Kill only 2020 while S3 is blocked - retry mechanism keeps exports alive
        # ZooKeeper operations (KILL) proceed quickly since only S3 is blocked
        node.query(f"KILL EXPORT PARTITION WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'")

    # wait for 2021 to finish
    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    # checking for the commit file because maybe the data file was too fast?
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '0\n', "Partition 2020 was written to S3, it was not killed as expected"
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2021_*', format=LineAsString)") != f'0\n', "Partition 2021 was not written to S3, but it should have been"

    # check system.replicated_partition_exports for the export, status should be KILLED
    assert node.query(f"SELECT status FROM system.replicated_partition_exports WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'") == 'KILLED\n', "Partition 2020 was not killed as expected"
    assert node.query(f"SELECT status FROM system.replicated_partition_exports WHERE partition_id = '2021' and source_table = '{mt_table}' and destination_table = '{s3_table}'") == 'COMPLETED\n', "Partition 2021 was not completed, this is unexpected"


def test_drop_source_table_during_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    # node2 = cluster.instances["replica2"]
    watcher_node = cluster.instances["watcher_node"]

    mt_table = "drop_source_table_during_export_mt_table"
    s3_table = "drop_source_table_during_export_s3_table"

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
            EXPORT PARTITION ID '2020' TO TABLE {s3_table};
            ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2021' TO TABLE {s3_table};
        """

        node.query(export_queries)

        # This should kill the background operations and drop the table
        node.query(f"DROP TABLE {mt_table}")

    # Sleep some time to let the export finish (assuming it was not properly cancelled)
    time.sleep(10)

    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_*', format=LineAsString)") == '0\n', "Background operations completed even with the table dropped"


def test_concurrent_exports_to_different_targets(cluster):
    node = cluster.instances["replica1"]

    mt_table = "concurrent_diff_targets_mt_table"
    s3_table_a = "concurrent_diff_targets_s3_a"
    s3_table_b = "concurrent_diff_targets_s3_b"

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

    mt_table = "failure_is_logged_in_system_table_mt_table"
    s3_table = "failure_is_logged_in_system_table_s3_table"

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

        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_max_retries=1;"
        )

        # Wait so that the export fails
        wait_for_export_status(node, mt_table, s3_table, "2020", "FAILED", timeout=60)

    # Network restored; verify the export is marked as FAILED in the system table
    # Also verify we captured at least one exception and no commit file exists
    status = node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )

    assert status.strip() == "FAILED", f"Expected FAILED status, got: {status!r}"

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

    mt_table = "inject_short_living_failures_mt_table"
    s3_table = "inject_short_living_failures_s3_table"

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

        # set big max_retries so that the export does not fail completely
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_max_retries=100;"
        )

        # wait only for a second to get at least one failure, but not enough to finish the export
        time.sleep(5)

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


def test_export_ttl(cluster):
    node = cluster.instances["replica1"]

    mt_table = "export_ttl_mt_table"
    s3_table = "export_ttl_s3_table"

    expiration_time = 3

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    # start export
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_manifest_ttl={expiration_time};")

    # assert that I get an error when trying to export the same partition again, query_and_get_error
    error = node.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table};")
    assert "Export with key" in error, "Expected error about expired export"

    # wait for the export to finish and for the manifest to expire
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")
    time.sleep(expiration_time * 2)

    # assert that the export succeeded, check the commit file
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '1\n', "Export did not succeed"

    # start export again
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # assert that the export succeeded, check the commit file
    # there should be two commit files now, one for the first export and one for the second export
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '2\n', "Export did not succeed"


def test_export_partition_file_already_exists_policy(cluster):
    node = cluster.instances["replica1"]

    mt_table = "export_partition_file_already_exists_policy_mt_table"
    s3_table = "export_partition_file_already_exists_policy_s3_table"

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

    # last but not least, let's try with the error policy
    # max retries = 1 so it fails fast
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1, export_merge_tree_part_file_already_exists_policy='error', export_merge_tree_partition_max_retries=1",
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


def test_export_partition_feature_is_disabled(cluster):
    replica_with_export_disabled = cluster.instances["replica_with_export_disabled"]

    mt_table = "export_partition_feature_is_disabled_mt_table"
    s3_table = "export_partition_feature_is_disabled_s3_table"

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

    mt_table = "permissions_mt_table"
    s3_table = "permissions_s3_table"

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

    mt_table = "multiple_exports_within_a_single_query_mt_table"
    s3_table = "multiple_exports_within_a_single_query_s3_table"

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

    mt_table = "pending_mutations_throw_partition_mt_table"
    s3_table = "pending_mutations_throw_partition_s3_table"

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

    mt_table = "pending_mutations_skip_partition_mt_table"
    s3_table = "pending_mutations_skip_partition_s3_table"

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

    mt_table = "pending_patches_throw_partition_mt_table"
    s3_table = "pending_patches_throw_partition_s3_table"

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

    mt_table = "pending_patches_skip_partition_mt_table"
    s3_table = "pending_patches_skip_partition_s3_table"

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

    mt_table = "mutations_after_export_partition_mt_table"
    s3_table = "mutations_after_export_partition_s3_table"

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

    mt_table = "patches_after_export_partition_mt_table"
    s3_table = "patches_after_export_partition_s3_table"

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

    mt_table = "mutation_in_partition_clause_mt_table"
    s3_table = "mutation_in_partition_clause_s3_table"

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

    mt_table = "mixed_computed_mt_table"
    s3_table = "mixed_computed_s3_table"

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
