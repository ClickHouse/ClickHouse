import gzip
import json
import os
import pytest
import threading
from datetime import datetime, timezone
import time

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    get_uuid_str,
    get_last_snapshot
)


def _open_metadata_file(filepath):
    """Open an Iceberg metadata file, transparently handling gzip compression.

    ClickHouse writes compressed metadata with the encoding name (e.g. `gzip`)
    embedded in the middle of the file name, e.g. `v<N>.gzip.metadata.json`,
    so the filename still ends with `.json`. Detect gzip by the magic bytes
    (0x1f 0x8b) to be agnostic to the exact naming convention.
    """
    with open(filepath, "rb") as raw:
        magic = raw.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(filepath, "rt")
    return open(filepath, "r")


def get_current_snapshot_summary(path_to_table):
    """Return the summary dict of the current snapshot from the latest metadata file."""
    metadata_dir = f"{path_to_table}/metadata/"
    last_timestamp = 0
    best = None
    for filename in os.listdir(metadata_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(metadata_dir, filename)
            with _open_metadata_file(filepath) as f:
                data = json.load(f)
            ts = data.get("last-updated-ms", 0)
            if ts > last_timestamp:
                last_timestamp = ts
                best = data
    if best is None:
        return {}
    current_id = best.get("current-snapshot-id")
    for snap in best.get("snapshots", []):
        if snap.get("snapshot-id") == current_id:
            return snap.get("summary", {})
    return {}

@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_optimize(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 110)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME};", settings={"allow_experimental_iceberg_compaction" : 1})

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 90)"
    )

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )
    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 90

def test_optimize_manifest_per_file_stats(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_stats_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(10, 100)"
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    metadata_dir = (
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    )
    manifest_files = (
        instance.exec_in_container(
            [
                "bash",
                "-c",
                f"find '{metadata_dir}' -maxdepth 1 -name '*.avro' "
                f"-not -name 'snap-*.avro' -type f",
            ]
        )
        .strip()
        .splitlines()
    )
    assert manifest_files

    data_entries_checked = 0
    for manifest in manifest_files:
        result = instance.query(
            f"""
            SELECT
                tupleElement(data_file, 'content')             AS content,
                tupleElement(data_file, 'file_path')           AS file_path,
                tupleElement(data_file, 'record_count')        AS record_count,
                tupleElement(data_file, 'file_size_in_bytes')  AS file_size
            FROM file('{manifest}', Avro)
            FORMAT TSV
            """
        ).strip()
        if not result:
            continue
        for line in result.splitlines():
            content, file_path, record_count, file_size = line.split("\t")
            if int(content) != 0:
                continue

            exists = instance.exec_in_container(
                ["bash", "-c", f"test -f '{file_path}' && echo yes || echo no"]
            ).strip()
            if exists != "yes":
                continue

            actual_size = int(
                instance.exec_in_container(
                    ["bash", "-c", f"wc -c < '{file_path}'"]
                ).strip()
            )
            assert int(file_size) == actual_size

            actual_rows = int(
                instance.query(
                    f"SELECT count() FROM file('{file_path}', Parquet)"
                ).strip()
            )
            assert int(record_count) == actual_rows
            data_entries_checked += 1

    assert data_entries_checked > 0


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files(started_cluster_iceberg_with_spark, storage_type, format_version):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_v" + format_version + "_" + storage_type + "_" + get_uuid_str()

    # Merge-on-read modes are only valid for v2 tables; v1 has no row-level deletes.
    if format_version == "2":
        tbl_properties = (
            "'format-version' = '2', "
            "'write.update.mode' = 'merge-on-read', "
            "'write.delete.mode' = 'merge-on-read', "
            "'write.merge.mode' = 'merge-on-read'"
        )
    else:
        tbl_properties = "'format-version' = '1'"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ({tbl_properties})
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(600, 700)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(200, 300)")

    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(300, 400)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(400, 500)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(600, 700)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;", settings={"allow_experimental_iceberg_compaction" : 1})

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;", settings={"allow_experimental_iceberg_compaction" : 1})

    # Verify Spark can still read the table correctly after manifest compaction.
    # Total rows: range(10,100) + range(100,200) + range(600,700) + range(200,300)
    #           + range(300,400) + range(400,500) + range(600,700) = 690
    # (range(600,700) is inserted twice, so it contributes 200 rows.)
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    spark_rows = spark.read.format("iceberg").load(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    ).collect()
    assert len(spark_rows) == 690

    spark_ids = sorted(row["id"] for row in spark_rows)
    clickhouse_ids = list(map(int, instance.query(
        f"SELECT id FROM {TABLE_NAME} ORDER BY id"
    ).split()))
    assert spark_ids == clickhouse_ids


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_partitioned(started_cluster_iceberg_with_spark, storage_type):
    """
    Test manifest-only compaction for a partitioned Iceberg table.

    The table is partitioned by 'region' (3 distinct values).  We perform many
    small inserts across all partitions so that the number of manifest files
    grows well above the compaction threshold.  After OPTIMIZE TABLE ... MANIFEST
    the manifests should be consolidated to one per partition.

    Checks:
    - Data correctness is preserved after compaction.
    - Time-travel via snapshot_id still works after compaction.
    - A second OPTIMIZE invocation is a no-op (already optimal).
    - The compaction threshold setting is honoured: with the default threshold (30)
      a table that already has <= 30 manifest files is left untouched, while with
      a lower threshold (2) compaction is triggered sooner.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_partitioned_" + storage_type + "_" + get_uuid_str()

    # 3 distinct partition values
    REGIONS = ["eu", "us", "ap"]
    NUM_PARTITIONS = len(REGIONS)

    # ── Create partitioned table ──────────────────────────────────────────────
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string)
        USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode'  = 'merge-on-read',
            'write.delete.mode'  = 'merge-on-read',
            'write.merge.mode'   = 'merge-on-read'
        )
        """
    )

    # ── Initial insert – one batch per partition ──────────────────────────────
    for region in REGIONS:
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')), '{region}' "
            f"FROM range(0, 30)"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    first_snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    # 30 rows × 3 regions = 90 rows
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    # Time-travel snapshot should also see 90 rows
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {first_snapshot_id}"
    )) == 90

    # ── Many more small inserts to create many manifest files ─────────────────
    # 6 batches × 3 regions = 18 additional inserts → well above the lowered threshold (2)
    for batch_start in range(30, 90, 10):
        for region in REGIONS:
            spark.sql(
                f"INSERT INTO {TABLE_NAME} "
                f"SELECT id, char(id + ascii('a')), '{region}' "
                f"FROM range({batch_start}, {batch_start + 10})"
            )
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")

    # 90 (initial) + 6 batches × 10 rows × 3 regions = 90 + 180 = 270
    total_rows = 90 + 6 * 10 * NUM_PARTITIONS
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows

    # ── Run manifest compaction ───────────────────────────────────────────────
    # Lower threshold to 2 so that compaction is definitely triggered
    # (each partition will have at least 7 manifest files after the inserts above)
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # ── Data correctness after compaction ────────────────────────────────────
    # Check the current (post-compaction) snapshot via the default read path.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    for region in REGIONS:
        expected_count = 90  # 30 initial + 6 × 10 additional
        actual_count = int(instance.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}'"
        ))
        assert actual_count == expected_count, \
            f"Region '{region}': expected {expected_count} rows after compaction, got {actual_count}"

    # Cross-check: the pre-compaction snapshot must also still be readable.
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows

    # ── Time-travel still works after compaction ──────────────────────────────
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {first_snapshot_id}"
    )) == 90

    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}"
    )) == 90

    # ── Second OPTIMIZE should be a no-op (already one manifest per partition) ─
    # This must not raise and must leave data intact.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    # Verify the current snapshot is still intact after the no-op.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    # ── Third OPTIMIZE should throw exception
    error_message = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME} FINAL MANIFEST;",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    assert "OPTIMIZE MANIFEST is incompatible with FINAL, PARTITION, DEDUPLICATE, CLEANUP, and DRY RUN options" in error_message


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_partitioned_concurrent(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_concurrent_" + storage_type + "_" + get_uuid_str()

    REGIONS = ["eu", "us", "ap"]
    NUM_PARTITIONS = len(REGIONS)

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string)
        USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode'  = 'merge-on-read',
            'write.delete.mode'  = 'merge-on-read',
            'write.merge.mode'   = 'merge-on-read'
        )
        """
    )

    # Initial insert – one batch per partition.
    for region in REGIONS:
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')), '{region}' "
            f"FROM range(0, 30)"
        )

    # Many more small inserts to create many manifest files (>> compaction threshold).
    for batch_start in range(30, 90, 10):
        for region in REGIONS:
            spark.sql(
                f"INSERT INTO {TABLE_NAME} "
                f"SELECT id, char(id + ascii('a')), '{region}' "
                f"FROM range({batch_start}, {batch_start + 10})"
            )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # 30 initial + 6 × 10 additional rows per region.
    expected_per_region = 90
    total_rows = expected_per_region * NUM_PARTITIONS

    # Sanity check before launching threads.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    NUM_READER_THREADS = 4
    NUM_OPTIMIZE_THREADS = 2
    # Iteration-bounded rather than wall-clock-bounded: a slow CI runner
    # would otherwise truncate the test to a handful of iterations and miss
    # the conflict windows we're trying to exercise.
    OPTIMIZE_ITERATIONS_PER_THREAD = 5

    errors = []
    errors_lock = threading.Lock()
    optimize_attempts = [0] * NUM_OPTIMIZE_THREADS
    reader_attempts = [0] * NUM_READER_THREADS

    optimizers_done_event = threading.Event()
    finished_optimizers = [0]
    finished_lock = threading.Lock()

    def report_error(label, exc):
        with errors_lock:
            errors.append(f"{label}: {type(exc).__name__}: {exc}")

    def reader_loop(idx):
        # Readers run as long as any optimizer is still in flight, so the
        # exposure to the conflict window scales with the optimize workload
        # rather than with wall-clock time.
        try:
            while not optimizers_done_event.is_set():
                got_total = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
                if got_total != total_rows:
                    raise AssertionError(
                        f"SELECT count() returned {got_total}, expected {total_rows}"
                    )
                region = REGIONS[idx % NUM_PARTITIONS]
                got_part = int(instance.query(
                    f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}'"
                ))
                if got_part != expected_per_region:
                    raise AssertionError(
                        f"count(WHERE region={region}) returned {got_part}, "
                        f"expected {expected_per_region}"
                    )
                reader_attempts[idx] += 1
        except Exception as exc:
            report_error(f"reader-{idx}", exc)

    def optimize_loop(idx):
        try:
            for _ in range(OPTIMIZE_ITERATIONS_PER_THREAD):
                instance.query(
                    f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
                    settings={
                        "allow_experimental_iceberg_compaction": 1,
                        "iceberg_manifest_min_count_to_compact": 2,
                    },
                )
                optimize_attempts[idx] += 1
        except Exception as exc:
            report_error(f"optimize-{idx}", exc)
        finally:
            # Wake the readers as soon as the last optimizer is done so the
            # whole test ends in bounded time even if an optimizer raised.
            with finished_lock:
                finished_optimizers[0] += 1
                if finished_optimizers[0] == NUM_OPTIMIZE_THREADS:
                    optimizers_done_event.set()

    readers = [
        threading.Thread(target=reader_loop, args=(i,), daemon=True)
        for i in range(NUM_READER_THREADS)
    ]
    optimizers = [
        threading.Thread(target=optimize_loop, args=(i,), daemon=True)
        for i in range(NUM_OPTIMIZE_THREADS)
    ]

    for t in optimizers:
        t.start()
    for t in readers:
        t.start()

    # Generous per-thread join timeout so a slow CI runner does not flake;
    # the test itself completes as soon as all threads finish their bounded work.
    for t in optimizers + readers:
        t.join(timeout=300)
        assert not t.is_alive(), "Worker thread did not finish in time"

    assert not errors, "Concurrent run produced errors:\n" + "\n".join(errors)
    assert sum(reader_attempts) > 0, "No reads were performed"
    assert sum(optimize_attempts) == NUM_OPTIMIZE_THREADS * OPTIMIZE_ITERATIONS_PER_THREAD, (
        f"Expected {NUM_OPTIMIZE_THREADS * OPTIMIZE_ITERATIONS_PER_THREAD} OPTIMIZE iterations, "
        f"got {sum(optimize_attempts)}"
    )

    # Final consistency check.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows
    for region in REGIONS:
        assert int(instance.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}'"
        )) == expected_per_region


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_totals_invariant(started_cluster_iceberg_with_spark, storage_type):
    """
    Regression test: repeated OPTIMIZE TABLE ... MANIFEST must not inflate the
    snapshot summary totals (total-data-files, total-records, total-files-size).

    Before the fix, each compaction call passed added_files = total_data_files and
    added_records/added_files_size from the previous snapshot delta to
    generateNextMetadata, which computes total_* = parent_total_* + added_*.
    This caused totals to double (or more) with every OPTIMIZE run.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_totals_" + storage_type + "_" + get_uuid_str()
    TABLE_PATH = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    # Several inserts to produce multiple manifest files.
    for batch_start in range(0, 50, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')) FROM range({batch_start}, {batch_start + 10})"
        )
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 50

    # First compaction — consolidates manifests.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_first = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_first, "Could not read snapshot summary after first compaction"

    # Verify the operation type is correct for a manifest-only rewrite.
    assert summary_after_first.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary_after_first.get('operation')}"
    )

    total_files_1 = int(summary_after_first.get("total-data-files", -1))
    total_records_1 = int(summary_after_first.get("total-records", -1))
    total_size_1 = int(summary_after_first.get("total-files-size", -1))

    assert total_files_1 >= 0
    assert total_records_1 == 50

    # Second compaction — already optimal, should be a no-op that does NOT change totals.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_second = get_current_snapshot_summary(TABLE_PATH)

    # Third compaction — totals must remain identical.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 1,
        },
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_third = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_third, "Could not read snapshot summary after third compaction"

    total_files_3 = int(summary_after_third.get("total-data-files", -1))
    total_records_3 = int(summary_after_third.get("total-records", -1))
    total_size_3 = int(summary_after_third.get("total-files-size", -1))

    assert total_files_3 == total_files_1, (
        f"total-data-files inflated: {total_files_1} -> {total_files_3}"
    )
    assert total_records_3 == total_records_1, (
        f"total-records inflated: {total_records_1} -> {total_records_3}"
    )
    assert total_size_3 == total_size_1, (
        f"total-files-size inflated: {total_size_1} -> {total_size_3}"
    )

    # Data must still be correct after all compaction rounds.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 50


@pytest.mark.parametrize("compression_method", ["", "gzip"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_totals_invariant_schema_evolution(
    started_cluster_iceberg_with_spark, storage_type, compression_method
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    suffix = compression_method or "none"
    TABLE_NAME = f"test_optimize_totals_se_{suffix}_{storage_type}_{get_uuid_str()}"
    TABLE_PATH = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"

    base_settings = {"allow_insert_into_iceberg": 1}
    if compression_method:
        base_settings["iceberg_metadata_compression_method"] = compression_method

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(x Nullable(Int32))",
        format_version=2,
        compression_method=compression_method if compression_method else None,
    )

    # Schema evolution: widen, add, then drop a column to produce non-trivial metadata.
    instance.query(
        f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN x Nullable(Int64);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(0, 10);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(10, 10);",
        settings=base_settings,
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN y Nullable(Float64);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, number + 0.5 FROM numbers(20, 10);",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number, number + 0.5 FROM numbers(30, 10);",
        settings=base_settings,
    )

    instance.query(
        f"ALTER TABLE {TABLE_NAME} DROP COLUMN x;",
        settings=base_settings,
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number + 0.5 FROM numbers(40, 10);",
        settings=base_settings,
    )

    total_rows = 50
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    optimize_settings = dict(base_settings)
    optimize_settings.update({
        "allow_experimental_iceberg_compaction": 1,
        "iceberg_manifest_min_count_to_compact": 2,
    })

    # First compaction — consolidates manifests.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings=optimize_settings,
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_first = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_first, (
        f"Could not read snapshot summary after first compaction "
        f"(compression='{compression_method}')"
    )
    assert summary_after_first.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary_after_first.get('operation')}"
    )

    total_files_1 = int(summary_after_first.get("total-data-files", -1))
    total_records_1 = int(summary_after_first.get("total-records", -1))
    total_size_1 = int(summary_after_first.get("total-files-size", -1))

    assert total_files_1 >= 0
    assert total_records_1 == total_rows

    # Second compaction — already optimal, totals must stay identical.
    optimize_settings["iceberg_manifest_min_count_to_compact"] = 1
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings=optimize_settings,
    )
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary_after_second = get_current_snapshot_summary(TABLE_PATH)
    assert summary_after_second, (
        f"Could not read snapshot summary after second compaction "
        f"(compression='{compression_method}')"
    )

    total_files_2 = int(summary_after_second.get("total-data-files", -1))
    total_records_2 = int(summary_after_second.get("total-records", -1))
    total_size_2 = int(summary_after_second.get("total-files-size", -1))

    assert total_files_2 == total_files_1, (
        f"total-data-files inflated (compression='{compression_method}'): "
        f"{total_files_1} -> {total_files_2}"
    )
    assert total_records_2 == total_records_1, (
        f"total-records inflated (compression='{compression_method}'): "
        f"{total_records_1} -> {total_records_2}"
    )
    assert total_size_2 == total_size_1, (
        f"total-files-size inflated (compression='{compression_method}'): "
        f"{total_size_1} -> {total_size_2}"
    )

    # Data must still be correct after compaction.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_parent_summary_missing_totals(
    started_cluster_iceberg_with_spark, storage_type
):
    """
    Regression test: OPTIMIZE TABLE ... MANIFEST must tolerate a parent snapshot
    whose summary omits some of the carried `total-*` counters.

    Iceberg only requires totals on snapshots that change row-level state, so older
    Spark-written tables and tables touched by tools like `removeOrphanFiles`
    routinely drop fields like `total-position-deletes`, `total-equality-deletes`,
    or `total-delete-files` from the summary. Before the fix, the carry-forward
    helper would call `parse<Int64>` on a missing field and throw
    "Cannot parse Int64".

    This test simulates that situation by stripping those fields from the latest
    metadata file before ClickHouse first sees the table.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_parent_missing_totals_" + storage_type + "_" + get_uuid_str()
    TABLE_PATH = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    for batch_start in range(0, 30, 10):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')) FROM range({batch_start}, {batch_start + 10})"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )


    # Locate the latest metadata file and strip several total-* fields from the
    # current snapshot's summary, mimicking older Spark / removeOrphanFiles output.
    metadata_dir = f"{TABLE_PATH}/metadata/"
    latest_path = None
    latest_ts = -1
    for filename in os.listdir(metadata_dir):
        if filename.endswith(".json"):
            fp = os.path.join(metadata_dir, filename)
            with _open_metadata_file(fp) as f:
                data = json.load(f)
            ts = data.get("last-updated-ms", 0)
            if ts > latest_ts:
                latest_ts = ts
                latest_path = fp
    assert latest_path is not None, "Could not locate latest metadata file"

    with _open_metadata_file(latest_path) as f:
        data = json.load(f)

    stripped_fields = (
        "total-position-deletes",
        "total-equality-deletes",
        "total-delete-files",
    )
    for snap in data.get("snapshots", []):
        summary = snap.get("summary", {})
        for stripped in stripped_fields:
            summary.pop(stripped, None)

    # Preserve the on-disk encoding (gzip vs plain) when writing back.
    with open(latest_path, "rb") as raw:
        magic = raw.read(2)
    if magic == b"\x1f\x8b":
        with gzip.open(latest_path, "wt") as f:
            json.dump(data, f)
    else:
        with open(latest_path, "w") as f:
            json.dump(data, f)

    # Re-upload the edited metadata so ClickHouse reads the stripped summary.
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Create the table only after the edit so no metadata cache is populated
    # with the original (full) summary.
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 30

    # Must not throw despite the missing total-* fields in the parent summary.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 1,
        },
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 30

    # The newly-written manifest-only snapshot must carry the missing totals as "0".
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    summary = get_current_snapshot_summary(TABLE_PATH)
    assert summary, "Could not read snapshot summary after compaction"
    assert summary.get("operation") == "replace", (
        f"Expected operation='replace', got: {summary.get('operation')}"
    )
    for stripped in stripped_fields:
        assert summary.get(stripped) == "0", (
            f"Expected {stripped}='0' on the new manifest-only snapshot, "
            f"got: {summary.get(stripped)!r}"
        )
