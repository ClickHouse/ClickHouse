import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


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
            "node1", 
            main_configs=["configs/named_collections.xml"],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_s3_table(node, s3_table):
    node.query(f"CREATE TABLE {s3_table} (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') PARTITION BY year")


def create_tables_and_insert_data(node, mt_table, s3_table):
    # enable_block_number_column and enable_block_offset_column are needed for patch parts support
    node.query(f"CREATE TABLE {mt_table} (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)


def test_drop_column_during_export_snapshot(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"mutations_snapshot_mt_table_{postfix}"
    s3_table = f"mutations_snapshot_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    # Block traffic to/from MinIO to force upload errors and retries, following existing S3 tests style
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    # Ensure export sees a consistent snapshot at start time even if we mutate the source later
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

        # Start export of 2020
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table};"
        )

        # Drop a column that is required for the export
        node.query(f"ALTER TABLE {mt_table} DROP COLUMN id")

        time.sleep(3)
        # assert the mutation has been applied AND the data has not been exported yet
        assert "Unknown expression identifier `id`" in node.query_and_get_error(f"SELECT id FROM {mt_table}"), "Column id is not removed"

    # Wait for export to finish and then verify destination still reflects the original snapshot (3 rows)
    time.sleep(5)
    assert node.query(f"SELECT count() FROM {s3_table} WHERE id >= 0") == '3\n', "Export did not preserve snapshot at start time after source mutation"


def test_add_column_during_export(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"add_column_during_export_mt_table_{postfix}"
    s3_table = f"add_column_during_export_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    # Block traffic to/from MinIO to force upload errors and retries, following existing S3 tests style
    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    # Ensure export sees a consistent snapshot at start time even if we mutate the source later
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

        # Start export of 2020
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table};"
        )

        node.query(f"ALTER TABLE {mt_table} ADD COLUMN id2 UInt64")

        time.sleep(3)

        # assert the mutation has been applied AND the data has not been exported yet
        assert node.query(f"SELECT count(id2) FROM {mt_table}") == '4\n', "Column id2 is not added"

    # Wait for export to finish and then verify destination still reflects the original snapshot (3 rows)
    time.sleep(5)
    assert node.query(f"SELECT count() FROM {s3_table} WHERE id >= 0") == '3\n', "Export did not preserve snapshot at start time after source mutation"
    assert "Unknown expression identifier `id2`" in node.query_and_get_error(f"SELECT id2 FROM {s3_table}"), "Column id2 is present in the exported data"


def test_pending_mutations_throw_before_export(cluster):
    """Test that pending mutations before export throw an error with default settings."""
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"pending_mutations_throw_mt_table_{postfix}"
    s3_table = f"pending_mutations_throw_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0")
    assert mutations.strip() != '0', "Mutation should be pending"

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table} SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )

    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error, f"Expected error about pending mutations, got: {error}"


def test_pending_mutations_skip_before_export(cluster):
    """Test that pending mutations before export are skipped with throw_on_pending_mutations=false."""
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"pending_mutations_skip_mt_table_{postfix}"
    s3_table = f"pending_mutations_skip_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0")
    assert mutations.strip() != '0', "Mutation should be pending"

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=false"
    )

    time.sleep(5)

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "101" not in result and "102" not in result and "103" not in result, \
        "Export should contain original data before mutation"
    assert "1\n2\n3" in result, "Export should contain original data"


def test_data_mutations_after_export_started(cluster):
    """Test that mutations applied after export starts don't affect the exported data."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"mutations_after_export_mt_table_{postfix}"
    s3_table = f"mutations_after_export_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

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
            f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table} "
            f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
        )

        node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    time.sleep(5)

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "1\n2\n3" in result, "Export should contain original data before mutation"
    assert "101" not in result, "Export should not contain mutated data"


def test_pending_patch_parts_throw_before_export(cluster):
    """Test that pending patch parts before export throw an error with default settings."""
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"pending_patches_throw_mt_table_{postfix}"
    s3_table = f"pending_patches_throw_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table}"
    )

    node.query(f"DROP TABLE {mt_table}")

    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error or "pending patch parts" in error.lower(), \
        f"Expected error about pending patch parts, got: {error}"


def test_pending_patch_parts_skip_before_export(cluster):
    """Test that pending patch parts before export are skipped with throw_on_pending_patch_parts=false."""
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")

    mt_table = f"pending_patches_skip_mt_table_{postfix}"
    s3_table = f"pending_patches_skip_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")
    
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_patch_parts=false"
    )

    time.sleep(5)

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "1\n2\n3" in result, "Export should contain original data before patch"

    node.query(f"DROP TABLE {mt_table}")
