import io
import json
import logging
import re
import time

import pytest
from avro.datafile import DataFileReader
from avro.io import DatumReader

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import (
    first_partition_id,
    make_iceberg_s3,
    make_rmt,
    unique_suffix,
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
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", (
        f"Unexpected data in Iceberg table:\n{result}"
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
        """
    )

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, iceberg_table, "2021", "COMPLETED")

    count_2020 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    count_2021 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2021").strip())

    assert count_2020 == 3, f"Expected 3 rows for year=2020, got {count_2020}"
    assert count_2021 == 1, f"Expected 1 row for year=2021, got {count_2021}"


def test_failure_is_logged_in_system_table(cluster):
    """
    When S3 is unreachable the export must be marked FAILED in
    system.replicated_partition_exports with a non-zero exception_count.
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

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} SETTINGS export_merge_tree_partition_max_retries = 1")

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

        wait_for_export_status(node, mt_table, iceberg_table, "2020", "FAILED", timeout=60)

    status = node.query(
        f"""
        SELECT status FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
          SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = 1
        """
    ).strip()
    assert status == "FAILED", f"Expected FAILED status, got: {status!r}"

    exception_count = int(node.query(
        f"""
        SELECT any(exception_count) FROM system.replicated_partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{iceberg_table}'
          AND partition_id = '2020'
          SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = 1
        """
    ).strip())
    assert exception_count > 0, "Expected non-zero exception_count in system.replicated_partition_exports"


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

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} SETTINGS export_merge_tree_partition_max_retries = 100")

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
          SETTINGS export_merge_tree_partition_system_table_prefer_remote_information = 1
        """
    ).strip())
    assert exception_count >= 1, "Expected at least one transient exception to be recorded"


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
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
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
        f" SETTINGS export_merge_tree_partition_max_retries = 50"
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
        f" SETTINGS export_merge_tree_partition_max_retries = 50")

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
    1. Compound identity (year, region)
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
            f"ALTER TABLE {mt} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}"
        )

    # 1. Compound identity: (year, region)
    cols = "id Int64, year Int32, region String"
    t = f"mt_acc_1_{uid}"; i = f"iceberg_acc_1_{uid}"
    make_rmt(node, t, cols, "(year, region)")
    node.query(f"INSERT INTO {t} VALUES (1, 2023, 'EU')")
    make_iceberg_s3(node, i, cols, "(year, region)")
    check_accepted(t, i, "compound identity (year, region)")

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
    Verify that mismatched partition specs are rejected with BAD_ARGUMENTS.

    Cases covered:
    1. Compound field order reversed: MergeTree (year, region) vs Iceberg (region, year)
    2. Transform mismatch on same column: year-transform vs identity
    3. Bucket count mismatch: bucket[8] vs bucket[16]
    4. Truncate width mismatch: truncate[4] vs truncate[8]
    5. Field-count mismatch: 2-field MergeTree vs 1-field Iceberg
    6. Unsupported MergeTree expression (intDiv — not an Iceberg transform)
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()

    def assert_rejected(mt, iceberg, description):
        # The compatibility check fires synchronously; any partition ID works here.
        pid = first_partition_id(node, mt)
        error = node.query_and_get_error(
            f"ALTER TABLE {mt} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}"
        )
        assert "BAD_ARGUMENTS" in error, (
            f"[{description}] Expected BAD_ARGUMENTS, got: {error!r}"
        )

    # 1. Compound field order reversed
    cols = "id Int64, year Int32, region String"
    t = f"mt_rej_1_{uid}"; i = f"iceberg_rej_1_{uid}"
    make_rmt(node, t, cols, "(year, region)")
    node.query(f"INSERT INTO {t} VALUES (1, 2020, 'EU')")
    make_iceberg_s3(node, i, cols, "(region, year)")
    assert_rejected(t, i, "compound field order reversed")

    # 2. Transform mismatch: MergeTree year-transform, Iceberg identity on same Date col
    cols = "id Int64, event_date Date"
    t = f"mt_rej_2_{uid}"; i = f"iceberg_rej_2_{uid}"
    make_rmt(node, t, cols, "toYearNumSinceEpoch(event_date)")
    node.query(f"INSERT INTO {t} VALUES (1, '2020-01-01')")
    make_iceberg_s3(node, i, cols, "event_date")   # identity, not year-transform
    assert_rejected(t, i, "year-transform vs identity on same column")

    # 3. Bucket count mismatch: bucket[8] vs bucket[16]
    cols = "id Int64, user_id Int64"
    t = f"mt_rej_3_{uid}"; i = f"iceberg_rej_3_{uid}"
    make_rmt(node, t, cols, "icebergBucket(8, user_id)")
    node.query(f"INSERT INTO {t} VALUES (1, 42)")
    make_iceberg_s3(node, i, cols, "icebergBucket(16, user_id)")
    assert_rejected(t, i, "bucket[8] vs bucket[16]")

    # 4. Truncate width mismatch: truncate[4] vs truncate[8]
    cols = "id Int64, category String"
    t = f"mt_rej_4_{uid}"; i = f"iceberg_rej_4_{uid}"
    make_rmt(node, t, cols, "icebergTruncate(4, category)")
    node.query(f"INSERT INTO {t} VALUES (1, 'clickhouse')")
    make_iceberg_s3(node, i, cols, "icebergTruncate(8, category)")
    assert_rejected(t, i, "truncate[4] vs truncate[8]")

    # 5. Field-count mismatch: MergeTree has 2 fields, Iceberg has 1
    cols = "id Int64, year Int32, region String"
    t = f"mt_rej_5_{uid}"; i = f"iceberg_rej_5_{uid}"
    make_rmt(node, t, cols, "(year, region)")
    node.query(f"INSERT INTO {t} VALUES (1, 2020, 'EU')")
    make_iceberg_s3(node, i, cols, "year")
    assert_rejected(t, i, "2-field MergeTree vs 1-field Iceberg")

    # 6. Unsupported MergeTree expression: intDiv(year, 100) is not an Iceberg transform
    cols = "id Int64, year Int32"
    t = f"mt_rej_6_{uid}"; i = f"iceberg_rej_6_{uid}"
    make_rmt(node, t, cols, "intDiv(year, 100)")
    node.query(f"INSERT INTO {t} VALUES (1, 2020)")
    make_iceberg_s3(node, i, cols, "year")
    assert_rejected(t, i, "unsupported MergeTree expression intDiv")


def test_partition_key_compatibility_check(cluster):
    """
    Verify that EXPORT PARTITION throws BAD_ARGUMENTS synchronously when the
    MergeTree partition key does not match the Iceberg table's partition spec,
    and is accepted without error when the keys match.

    Three cases:
    1. Column mismatch   – MergeTree PARTITION BY year, Iceberg PARTITION BY id
    2. Count mismatch    – MergeTree PARTITION BY year, Iceberg unpartitioned
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
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_col_mismatch}"
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition column mismatch, got: {error!r}"
    )

    # --- Case 2: Iceberg unpartitioned but MergeTree PARTITION BY year ---
    iceberg_count_mismatch = f"iceberg_count_mismatch_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_count_mismatch}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_count_mismatch}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        SETTINGS s3_retry_attempts = 3
        """
    )
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_count_mismatch}"
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition count mismatch, got: {error!r}"
    )

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
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_match}"
    )


def test_export_ttl(cluster):
    """
    After a manifest TTL expires the same partition can be re-exported, and the
    new data is appended to (or replaces) what is in the Iceberg table.
    """
    node = cluster.instances["replica1"]
    ttl_seconds = 3

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    # First export.
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} "
        f"SETTINGS export_merge_tree_partition_manifest_ttl = {ttl_seconds}"
    )

    # A second export before the TTL expires must be rejected.
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
    )
    assert "Export with key" in error, f"Expected duplicate-export error before TTL, got: {error}"

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count_after_first = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count_after_first == 3, f"Expected 3 rows after first export, got {count_after_first}"

    # Wait for the manifest TTL to expire.
    time.sleep(ttl_seconds * 2)

    # Second export must be accepted now.
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")


def test_export_data_files_are_not_cleaned_up_on_commit_failure(cluster):
    """
    Verify that the data files are not cleaned up on commit failure and the export is retried.
    This is to avoid data loss.

    If the data files were cleaned up, a retry would commit a new snapshot that points to dangling references.
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"
    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query("SYSTEM ENABLE FAILPOINT iceberg_writes_non_retry_cleanup")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}")
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    assert count == 3, f"Expected 3 rows after first export, got {count}"


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
      - the export is marked COMPLETED because the idempotency check on retry
        detects that the transaction is already committed and returns success;
      - all exported rows are visible through the Iceberg table.
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"
    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"])

    node.query("SYSTEM ENABLE FAILPOINT iceberg_writes_post_publish_throw")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}")
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


def test_export_task_timeout_kills_stuck_pending_task(cluster):
    """
    Verify that export_merge_tree_partition_task_timeout_seconds auto-kills a task
    that remains PENDING past the deadline, transitioning it to KILLED with a
    descriptive last_exception.

    The export_partition_commit_always_throw failpoint wedges the task in the
    commit retry loop (REGULAR failpoint, fires on every commit attempt). A very
    large max_retries budget prevents the commit-attempts path from transitioning
    to FAILED before the timeout fires, so the timeout branch in tryCleanup is
    the actual mechanism under test.
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
            f"          export_merge_tree_partition_max_retries = 1000000,"
            f"          export_merge_tree_partition_manifest_ttl = 3600"
        )

        # Timeout budget must cover: the 5s task timeout + one manifest-updating
        # poll cycle (~30s) + watch propagation. 90s is safe.
        wait_for_export_status(
            node, mt_table, iceberg_table, "2020",
            expected_status="KILLED",
            timeout=90,
        )

        # TODO: system.replicated_partition_exports does not currently surface
        # last_exception / exception_count reliably (the engine's aggregation
        # from exceptions_per_replica is incomplete). Read the raw znode via
        # system.zookeeper until that is fixed.
        export_key = f"2020_default.{iceberg_table}"
        last_exception_path = (
            f"/clickhouse/tables/{mt_table}/exports/{export_key}"
            f"/exceptions_per_replica/replica1/last_exception"
        )
        last_exception = node.query(
            f"""
            SELECT value FROM system.zookeeper
            WHERE path = '{last_exception_path}' AND name = 'exception'
            """
        ).strip()
        assert "timed out" in last_exception, (
            f"Expected last_exception znode to mention the timeout reason, got: {last_exception!r}"
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
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}"
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
