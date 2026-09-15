import uuid

import pytest

from helpers.export_partition_helpers import (
    EXTRA_SOURCE_COLUMN_MODES,
    make_rmt,
    skip_if_remote_database_disk_enabled,
    wait_for_export_status,
    wait_for_export_to_start,
)
from helpers.network import PartitionManager

from .common import (
    create_s3_table,
    create_tables_and_insert_data,
)

CLUSTER_INSTANCES = ["replica1", "replica2", "watcher_node", "shard1_replica1", "shard2_replica1"]

# `EXPORT PARTITION` behavior that only exists with cross-replica coordination: assisting and
# non-initiating replicas, restarts mid-export, sharded destinations and the macros resolved from
# a `Replicated` database. These always use a `ReplicatedMergeTree` source.


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
