import logging
import time
import uuid
from typing import NamedTuple

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


class RejectedPartExportCase(NamedTuple):
    src_columns: str
    src_partition_by: str
    dst_columns: str
    dst_partition_by: str
    insert_values: str
    error_substrings: tuple = ()
    partition_strategy: str = "hive"


REJECTED_PART_EXPORT_CASES = [
    pytest.param(
        RejectedPartExportCase(
            src_columns="a Int32, b Int32",
            src_partition_by="a",
            dst_columns="b Int32, a Int32",
            dst_partition_by="a",
            insert_values="(1, 1), (1, 2)",
            error_substrings=(
                "partition key column 'a' is at position 0 in the source table",
            ),
        ),
        id="same_partition_key_different_column_order_single_column",
    ),
    pytest.param(
        RejectedPartExportCase(
            src_columns="a Int32, b Int32, c Int32, val String",
            src_partition_by="(a, b, c)",
            dst_columns="c Int32, b Int32, a Int32, val String",
            dst_partition_by="(a, b, c)",
            insert_values="(1, 1, 1, 'x'), (1, 1, 1, 'y')",
            error_substrings=(
                "partition key column 'a' is at position 0 in the source table",
            ),
        ),
        id="same_partition_key_different_column_order_multi_column",
    ),
    pytest.param(
        RejectedPartExportCase(
            src_columns="a Int32, b Int32, c Int32, val String",
            src_partition_by="(a, b, c)",
            dst_columns="a Int32, b Int32, c Int32, val String",
            dst_partition_by="(c, b, a)",
            insert_values="(1, 2, 3, 'x')",
            error_substrings=(
                "Tables have different partition key",
            ),
        ),
        id="multi_column_partition_key_order_mismatch",
    ),
    pytest.param(
        RejectedPartExportCase(
            src_columns="a Int32, b Int32, c Int32, val String",
            src_partition_by="(a, b, c)",
            dst_columns="a Int32, b Int32, c Int32, val String",
            dst_partition_by="(a, b)",
            insert_values="(1, 2, 3, 'x')",
            error_substrings=(
                "Tables have different partition key",
            ),
        ),
        id="multi_column_partition_key_fewer_in_destination",
    ),
    pytest.param(
        RejectedPartExportCase(
            src_columns="a Int32, b Int32, c Int32, val String",
            src_partition_by="(a, b)",
            dst_columns="a Int32, b Int32, c Int32, val String",
            dst_partition_by="(a, b, c)",
            insert_values="(1, 2, 3, 'x')",
            error_substrings=(
                "Tables have different partition key",
            ),
        ),
        id="multi_column_partition_key_more_in_destination",
    ),
    pytest.param(
        RejectedPartExportCase(
            src_columns="ts DateTime, category String, decoy DateTime, val String",
            src_partition_by="(toYYYYMM(ts), category)",
            dst_columns="decoy DateTime, category String, ts DateTime, val String",
            dst_partition_by="(toYYYYMM(ts), category)",
            insert_values=(
                "('2024-03-05 15:00:00', 'category', "
                "'2024-03-06 15:00:00', 'x')"
            ),
            error_substrings=(
                "partition key column 'ts' is at position 0 in the source table",
            ),
            partition_strategy="wildcard",
        ),
        id="function_and_column_partition_key_owner_reordered",
    ),
    pytest.param(
        RejectedPartExportCase(
            src_columns=(
                "t Tuple(ts DateTime, value Int32), category String, "
                "decoy Tuple(ts DateTime, value Int32), val String"
            ),
            src_partition_by="(toYYYYMM(t.ts), category)",
            dst_columns=(
                "decoy Tuple(ts DateTime, value Int32), category String, "
                "t Tuple(ts DateTime, value Int32), val String"
            ),
            dst_partition_by="(toYYYYMM(t.ts), category)",
            insert_values=(
                "(('2024-03-05 15:00:00', 1), 'category', "
                "('2024-03-06 15:00:00', 2), 'x')"
            ),
            error_substrings=(
                "partition key column 't' is at position 0 in the source table",
            ),
            partition_strategy="wildcard",
        ),
        id="function_over_subcolumn_partition_key_owner_reordered",
    ),
]


@pytest.mark.parametrize("case", REJECTED_PART_EXPORT_CASES)
def test_export_part_partition_key_mismatch_variants_are_rejected(cluster, case):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"rejected_mt_table_{postfix}"
    s3_table = f"rejected_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} ({case.src_columns})
        ENGINE = MergeTree()
        PARTITION BY {case.src_partition_by}
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    filename = (
        f"{s3_table}/{{_partition_id}}/{{_file}}"
        if case.partition_strategy == "wildcard"
        else s3_table
    )
    node.query(f"""
        CREATE TABLE {s3_table} ({case.dst_columns})
        ENGINE = S3(s3_conn, filename='{filename}', format=Parquet, partition_strategy='{case.partition_strategy}')
        PARTITION BY {case.dst_partition_by}
    """)

    node.query(f"INSERT INTO {mt_table} VALUES {case.insert_values}")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}")
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error}"
    for substring in case.error_substrings:
        assert substring in error, f"Expected {substring!r} in error, got: {error}"

    if case.partition_strategy == "hive":
        count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
        assert count == 0, (
            f"Expected 0 rows in destination after rejected export, got {count}"
        )


def test_export_part_multi_column_partition_key_success(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multi_pkey_ok_mt_table_{postfix}"
    s3_table = f"multi_pkey_ok_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = MergeTree()
        PARTITION BY (a, b, c)
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY (a, b, c)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (1, 2, 3, 'y')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    node.query(f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}")

    time.sleep(5)

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {s3_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n1\t2\t3\ty", f"Unexpected exported data:\n{result}"


@pytest.mark.parametrize(
    "owner_name, source_type, destination_type, partition_by, insert_value",
    [
        pytest.param(
            "t",
            "Tuple(a Int32, b Int32)",
            "Tuple(b Int32, a Int32)",
            "t.a",
            "(1, 99)",
            id="named_subcolumn",
        ),
        pytest.param(
            "t",
            "Tuple(a Int32, b Int32)",
            "Tuple(b Int32, a Int32)",
            "tupleElement(t, 1)",
            "(1, 99)",
            id="positional_tuple_element",
        ),
        pytest.param(
            "arr",
            "Array(Tuple(a Int32, b Int32))",
            "Array(Tuple(b Int32, a Int32))",
            "tupleElement(arr[1], 'a')",
            "[(1, 99)]",
            id="tuple_nested_in_array",
        ),
        pytest.param(
            "m",
            "Map(String, Tuple(a Int32, b Int32))",
            "Map(String, Tuple(b Int32, a Int32))",
            "tupleElement(m['key'], 'a')",
            "map('key', (1, 99))",
            id="tuple_nested_in_map_value",
        ),
    ],
)
def test_export_part_tuple_fields_reordered_for_partition_key_is_rejected(
    cluster,
    owner_name,
    source_type,
    destination_type,
    partition_by,
    insert_value,
):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"reordered_tuple_mt_table_{postfix}"
    s3_table = f"reordered_tuple_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} ({owner_name} {source_type}, val String)
        ENGINE = MergeTree()
        PARTITION BY {partition_by}
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} ({owner_name} {destination_type}, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY {partition_by}
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ({insert_value}, 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error and "different Tuple element layout" in error, (
        f"Expected export to reject reordered named `Tuple` fields used by "
        f"`PARTITION BY {partition_by}`, got: {error!r}"
    )


def test_export_part_unnamed_tuple_partition_key_owner_matching_named_destination_is_allowed(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"unnamed_tuple_ok_mt_table_{postfix}"
    s3_table = f"unnamed_tuple_ok_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (t Tuple(Int32, Int32), val String)
        ENGINE = MergeTree()
        PARTITION BY tupleElement(t, 1)
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (t Tuple(x Int32, y Int32), val String)
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY tupleElement(t, 1)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((1, 99), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    node.query(f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}")


def test_export_part_subcolumn_partition_key_different_subcolumn_is_rejected(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"subcol_diff_subcol_mt_table_{postfix}"
    s3_table = f"subcol_diff_subcol_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Tuple(b Int32, c Int32), val String)
        ENGINE = MergeTree()
        PARTITION BY a.b
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (a Tuple(b Int32, c Int32), val String)
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY a.c
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((1, 2), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert (
        "BAD_ARGUMENTS" in error
        and "Tables have different partition key"
        in error
    ), (
        f"Both tables declare `a` as the same Tuple(b Int32, c Int32) (so the column-cast "
        f"check passes and the owner-name-only `partition_key_owner_columns` contains "
        f"only `a`, so `verifyExportSchemaCastable` cannot distinguish `a.b` from "
        f"`a.c`), but the source "
        f"partitions by `a.b` and the destination by `a.c` — a genuinely different "
        f"partition key that must be caught by the `PARTITION BY` AST comparison; "
        f"got: {error!r}"
    )


def test_export_part_tuple_subcolumn_partition_key_owner_column_reordered_is_rejected(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"tuple_subcol_owner_mt_table_{postfix}"
    s3_table = f"tuple_subcol_owner_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (t Tuple(a Int32, b Int32), decoy Tuple(a Int32, b Int32), val String)
        ENGINE = MergeTree()
        PARTITION BY t.a
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (decoy Tuple(a Int32, b Int32), t Tuple(a Int32, b Int32), val String)
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY t.a
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((1, 100), (2, 200), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error and "partition key column" in error, (
        f"Expected export to reject `t` and `decoy` swapping positions around the "
        f"partition key column `t.a`, the same way a plain (non-tuple) partition key "
        f"column position swap is rejected; got: {error!r}"
    )


def test_export_part_multiple_partition_key_subcolumns_with_same_owner_reordered_is_rejected(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"same_owner_subcolumns_mt_table_{postfix}"
    s3_table = f"same_owner_subcolumns_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (
            t Tuple(a Int32, b Int32),
            decoy Tuple(a Int32, b Int32),
            val String
        )
        ENGINE = MergeTree()
        PARTITION BY (t.a, t.b)
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (
            decoy Tuple(a Int32, b Int32),
            t Tuple(a Int32, b Int32),
            val String
        )
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY (t.a, t.b)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((1, 10), (2, 20), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error and "partition key column 't'" in error, (
        f"Expected both `t.a` and `t.b` to resolve to the same top-level owner `t` "
        f"and reject swapping `t` with `decoy`; got: {error!r}"
    )


def test_export_part_multi_level_subcolumn_partition_key_owner_reordered_is_rejected(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"nested_subcol_owner_mt_table_{postfix}"
    s3_table = f"nested_subcol_owner_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (
            t Tuple(x Tuple(a Int32, b Int32), c Int32),
            decoy Tuple(x Tuple(a Int32, b Int32), c Int32),
            val String
        )
        ENGINE = MergeTree()
        PARTITION BY t.x.a
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (
            decoy Tuple(x Tuple(a Int32, b Int32), c Int32),
            t Tuple(x Tuple(a Int32, b Int32), c Int32),
            val String
        )
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY t.x.a
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((((1, 100), 1000)), (((2, 200), 2000)), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error and "partition key column" in error, (
        f"Expected export to reject `t` and `decoy` swapping positions around the "
        f"two-level-deep partition key column `t.x.a`. This only works if "
        f"`getNameInStorage` resolves all the way to the top-level column `t`, not to "
        f"the intermediate level `t.x`; got: {error!r}"
    )


def test_export_part_multiple_subcolumn_partition_keys_owner_reordered_is_rejected(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multi_subcol_key_mt_table_{postfix}"
    s3_table = f"multi_subcol_key_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (
            t Tuple(a Int32, x Int32),
            u Tuple(b Int32, y Int32),
            decoy Tuple(b Int32, y Int32),
            val String
        )
        ENGINE = MergeTree()
        PARTITION BY (t.a, u.b)
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (
            t Tuple(a Int32, x Int32),
            decoy Tuple(b Int32, y Int32),
            u Tuple(b Int32, y Int32),
            val String
        )
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY (t.a, u.b)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((1, 10), (2, 20), (3, 30), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error and "partition key column 'u'" in error, (
        f"`t` (owner of key part `t.a`) stays at position 0 on both sides, so the guard "
        f"must independently catch `u` (owner of key part `u.b`) swapping positions "
        f"with `decoy` — a partition key with two subcolumn-owning columns must have "
        f"both validated, not just the first one encountered; got: {error!r}"
    )


def test_export_part_mixed_flat_and_subcolumn_partition_key_flat_part_reordered_is_rejected(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mixed_key_mt_table_{postfix}"
    s3_table = f"mixed_key_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Int32, t Tuple(b Int32, c Int32), decoy Int32, val String)
        ENGINE = MergeTree()
        PARTITION BY (a, t.b)
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (decoy Int32, t Tuple(b Int32, c Int32), a Int32, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY (a, t.b)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES (1, (2, 3), 4, 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error and "partition key column 'a'" in error, (
        f"`t` (owner of key part `t.b`) stays at position 1 on both sides, so the guard "
        f"must independently catch the plain, non-tuple key part `a` swapping positions "
        f"with `decoy` — the pre-existing flat-column check and the new subcolumn-owner "
        f"resolution must both keep working when combined in one `PARTITION BY` "
        f"expression; got: {error!r}"
    )


def test_export_part_subcolumn_partition_key_owner_reordered_rejected_even_with_allow_lossy_cast(cluster):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"lossy_owner_mt_table_{postfix}"
    s3_table = f"lossy_owner_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (t Tuple(a Int32, b Int32), decoy Tuple(a Int32, b Int32), val String)
        ENGINE = MergeTree()
        PARTITION BY t.a
        ORDER BY tuple()
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (decoy Tuple(a Int32, b Int32), t Tuple(a Int32, b Int32), val String)
        ENGINE = S3(s3_conn, filename='{s3_table}/{{_partition_id}}/{{_file}}', format=Parquet, partition_strategy='wildcard')
        PARTITION BY t.a
    """)

    node.query(f"INSERT INTO {mt_table} VALUES ((1, 100), (2, 200), 'x')")

    part_name = node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PART '{part_name}' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_allow_lossy_cast = 1"
    )
    assert "BAD_ARGUMENTS" in error and "partition key column" in error, (
        f"The partition-key position/name guard is checked before the "
        f"`allow_lossy_cast` early-continue in verifyExportSchemaCastable, so setting "
        f"`export_merge_tree_part_allow_lossy_cast = 1` must not suppress the rejection "
        f"of `t`/`decoy` swapping positions around the partition key column `t.a`; "
        f"got: {error!r}"
    )
