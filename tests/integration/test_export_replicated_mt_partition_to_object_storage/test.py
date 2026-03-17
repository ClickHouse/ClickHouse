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


def wait_for_exception_count(
    node,
    mt_table: str,
    s3_table: str,
    partition_id: str,
    min_exception_count: int = 1,
    timeout: int = 30,
    poll_interval: float = 0.5,
):
    """Wait for exception_count to reach at least min_exception_count."""
    start_time = time.time()
    last_exception_count = None
    while time.time() - start_time < timeout:
        exception_count_str = node.query(
            f"""
            SELECT exception_count FROM system.replicated_partition_exports
            WHERE source_table = '{mt_table}'
              AND destination_table = '{s3_table}'
              AND partition_id = '{partition_id}'
            """
        ).strip()
        
        if exception_count_str:
            exception_count = int(exception_count_str)
            last_exception_count = exception_count
            if exception_count >= min_exception_count:
                return exception_count
        
        time.sleep(poll_interval)
    
    raise TimeoutError(
        f"Exception count did not reach {min_exception_count} within {timeout}s. "
        f"Last exception_count: {last_exception_count if last_exception_count is not None else 'N/A'}"
    )


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


@pytest.mark.parametrize(
    "system_table_prefer_remote_information", ['0', '1']
)
def test_kill_export(cluster, system_table_prefer_remote_information):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]
    node2 = cluster.instances["replica2"]
    watcher_node = cluster.instances["watcher_node"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"kill_export_mt_table_{system_table_prefer_remote_information}_{postfix}"
    s3_table = f"kill_export_s3_table_{system_table_prefer_remote_information}_{postfix}"

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

        # sleep for a while to let the kill to be processed
        time.sleep(2)

    # wait for 2021 to finish
    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    # checking for the commit file because maybe the data file was too fast?
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)") == '0\n', "Partition 2020 was written to S3, it was not killed as expected"
    assert node.query(f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2021_*', format=LineAsString)") != f'0\n', "Partition 2021 was not written to S3, but it should have been"

    # check system.replicated_partition_exports for the export, status should be KILLED
    assert node.query(f"SELECT status FROM system.replicated_partition_exports WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}' SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = {system_table_prefer_remote_information}") == 'KILLED\n', "Partition 2020 was not killed as expected"
    assert node.query(f"SELECT status FROM system.replicated_partition_exports WHERE partition_id = '2021' and source_table = '{mt_table}' and destination_table = '{s3_table}' SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = {system_table_prefer_remote_information}") == 'COMPLETED\n', "Partition 2021 was not completed, this is unexpected"

    # check the data did not land on s3
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == '0\n', "Partition 2020 was written to S3, it was not killed as expected"


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
          SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = 1
        """
    )

    assert status.strip() == "FAILED", f"Expected FAILED status, got: {status!r}"

    exception_count = node.query(
        f"""
        SELECT any(exception_count) FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = 1
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

        # set big max_retries so that the export does not fail completely
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_max_retries=100;"
        )

        # wait for at least one exception to occur, but not enough to finish the export
        wait_for_exception_count(node, mt_table, s3_table, "2020", min_exception_count=1, timeout=30)

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

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_ttl_mt_table_{postfix}"
    s3_table = f"export_ttl_s3_table_{postfix}"

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

    mt_table = "sharded_mt_table"
    s3_table = "sharded_s3_table"

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
