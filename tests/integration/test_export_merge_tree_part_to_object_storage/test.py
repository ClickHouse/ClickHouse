import logging
import pytest
import random
import string
import time
from typing import Optional
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

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
    node.query(f"CREATE TABLE {mt_table} (id UInt64, year UInt16) ENGINE = MergeTree() PARTITION BY year ORDER BY tuple()")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)


def test_drop_column_during_export_snapshot(cluster):
    node = cluster.instances["node1"]

    mt_table = "mutations_snapshot_mt_table"
    s3_table = "mutations_snapshot_s3_table"

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
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
            "protocol": "tcp",
        }
        pm.add_rule(pm_rule_reject_responses)

        # Block requests to MinIO (destination: MinIO, destination_port: minio_port)
        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
            "protocol": "tcp",
        }
        pm.add_rule(pm_rule_reject_requests)

        # Start export of 2020
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table} SETTINGS allow_experimental_export_merge_tree_part = 1;"
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
    node = cluster.instances["node1"]

    mt_table = "add_column_during_export_mt_table"
    s3_table = "add_column_during_export_s3_table"

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
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
            "protocol": "tcp",
        }
        pm.add_rule(pm_rule_reject_responses)

        # Block requests to MinIO (destination: MinIO, destination_port: minio_port)
        pm_rule_reject_requests = {
            "instance": node,
            "destination": minio_ip,
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
            "protocol": "tcp",
        }
        pm.add_rule(pm_rule_reject_requests)

        # Start export of 2020
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PART '2020_1_1_0' TO TABLE {s3_table} SETTINGS allow_experimental_export_merge_tree_part = 1;"
        )

        node.query(f"ALTER TABLE {mt_table} ADD COLUMN id2 UInt64")

        time.sleep(3)

        # assert the mutation has been applied AND the data has not been exported yet
        assert node.query(f"SELECT count(id2) FROM {mt_table}") == '4\n', "Column id2 is not added"

    # Wait for export to finish and then verify destination still reflects the original snapshot (3 rows)
    time.sleep(5)
    assert node.query(f"SELECT count() FROM {s3_table} WHERE id >= 0") == '3\n', "Export did not preserve snapshot at start time after source mutation"
    assert "Unknown expression identifier `id2`" in node.query_and_get_error(f"SELECT id2 FROM {s3_table}"), "Column id2 is present in the exported data"
