import logging

import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str,
)

NUMBER_OF_MANIFESTS = 5
ROWS_PER_MANIFEST = 100

# Must match the delay the `iceberg_slow_manifest_read` failpoint injects.
SLEEP_PER_MANIFEST_SECONDS = 0.40

# Serial reads cost one delay each; prefetching overlaps them. The bound sits between.
SERIAL_SECONDS = NUMBER_OF_MANIFESTS * SLEEP_PER_MANIFEST_SECONDS
MAX_SELECT_SECONDS = 0.8 * SERIAL_SECONDS

# Prefetching every manifest leaves one delay on the critical path instead of one per two
# manifests. A ratio, unlike the absolute bound above, does not depend on the machine speed.
MAX_DEEP_TO_SHALLOW_RATIO = 0.7


def elapsed(node, query, **kwargs):
    """Runs the query and returns its duration in seconds and the number of rows it read."""
    query_id = get_uuid_str()
    node.query(query, query_id=query_id, **kwargs)
    node.query("SYSTEM FLUSH LOGS query_log")
    result = node.query(
        f"""SELECT query_duration_ms / 1000.0, read_rows FROM system.query_log
        WHERE type = 'QueryFinish' AND query_id = '{query_id}' LIMIT 1""")
    duration, read_rows = result.split()
    return float(duration), int(read_rows)


@pytest.fixture(scope="module")
def manifest_heavy_table(started_cluster_iceberg_with_spark):
    """Creates a manifest-heavy Iceberg table and returns its `icebergS3(...)` expression.

    Module scoped: the Spark inserts dominate the runtime, and `--dist=loadfile` keeps all tests
    of one file on the same worker, so the table is built once and shared.
    """
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_manifest_read_performance_" + get_uuid_str()

    # Create Iceberg table. Properties ensure (a) manifests don't get merged and
    # (b) old metadata.json files get deleted.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id BIGINT)
        USING iceberg
        partitioned by (id)
        TBLPROPERTIES ('commit.manifest-merge.enabled' = 'false',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10')""")

    # Each commit writes FILES_PER_MANIFEST data files in a single append, producing one
    # manifest with that many entries, to create a manifest-heavy table.
    for i in range(NUMBER_OF_MANIFESTS):
        spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id FROM range({ROWS_PER_MANIFEST});")
        logging.info("Inserted %s/%s commits", i + 1, NUMBER_OF_MANIFESTS)

    # Copy files Spark created to minio.
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "s3",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Generate icebergS3(...) expression.
    return get_creation_expression("s3", TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True)


def test_manifest_read_performance(started_cluster_iceberg_with_spark, manifest_heavy_table):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    # Skip timing asserts on slow builds.
    slow_build = instance.is_built_with_sanitizer() or instance.is_built_with_llvm_coverage()

    # Read from table. Simulate realistic manifest read latency.
    instance.query("SYSTEM ENABLE FAILPOINT iceberg_slow_manifest_read")
    try:
        duration, _ = elapsed(
            instance,
            f"SELECT * FROM {manifest_heavy_table} SETTINGS use_iceberg_metadata_files_cache = 0 FORMAT Null",
        )
    finally:
        instance.query("SYSTEM DISABLE FAILPOINT iceberg_slow_manifest_read")

    logging.info("SELECT * over %s manifests took %.3f seconds", NUMBER_OF_MANIFESTS, duration)

    if slow_build:
        logging.info(
            "Instrumented build: skipping the %.3fs wall-clock bound", MAX_SELECT_SECONDS
        )
        return

    assert duration < MAX_SELECT_SECONDS, (
        f"SELECT * over a table with ~{NUMBER_OF_MANIFESTS} manifests took "
        f"{duration:.3f}s, exceeding the {MAX_SELECT_SECONDS}s bound. Reading them serially "
        f"would take about {SERIAL_SECONDS}s, so the manifests were likely not prefetched"
    )


def test_manifest_prefetch_count(started_cluster_iceberg_with_spark, manifest_heavy_table):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    # Skip timing asserts on slow builds.
    slow_build = instance.is_built_with_sanitizer() or instance.is_built_with_llvm_coverage()

    select = f"SELECT * FROM {manifest_heavy_table} SETTINGS use_iceberg_metadata_files_cache = 0"

    # Warm the data file cache so both measured runs start from the same state. Manifests are
    # unaffected: with no metadata cache they are re-read, and delayed, on every query.
    instance.query(f"{select} FORMAT Null")

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_slow_manifest_read")
    try:
        shallow, shallow_rows = elapsed(
            instance, f"{select}, iceberg_prefetch_manifest_files = 1 FORMAT Null"
        )
        deep, deep_rows = elapsed(
            instance,
            f"{select}, iceberg_prefetch_manifest_files = {NUMBER_OF_MANIFESTS} FORMAT Null",
        )
    finally:
        instance.query("SYSTEM DISABLE FAILPOINT iceberg_slow_manifest_read")

    logging.info(
        "SELECT * over %s manifests took %.3fs prefetching one manifest and %.3fs prefetching %s",
        NUMBER_OF_MANIFESTS, shallow, deep, NUMBER_OF_MANIFESTS,
    )

    # A deeper prefetch queue must not drop or duplicate manifest entries.
    assert shallow_rows == deep_rows == NUMBER_OF_MANIFESTS * ROWS_PER_MANIFEST

    # A depth far larger than the number of manifests is harmless.
    _, huge_depth_rows = elapsed(
        instance, f"{select}, iceberg_prefetch_manifest_files = 1000 FORMAT Null"
    )
    assert huge_depth_rows == NUMBER_OF_MANIFESTS * ROWS_PER_MANIFEST

    # The setting is NonZeroUInt64: prefetching cannot be turned off entirely.
    error = instance.query_and_get_error(
        f"{select}, iceberg_prefetch_manifest_files = 0 FORMAT Null"
    )
    assert "A setting's value has to be greater than 0" in error, error

    if slow_build:
        logging.info("Instrumented build: skipping the wall-clock comparison")
        return

    assert deep < MAX_DEEP_TO_SHALLOW_RATIO * shallow, (
        f"SELECT * over ~{NUMBER_OF_MANIFESTS} manifests took {deep:.3f}s with "
        f"iceberg_prefetch_manifest_files = {NUMBER_OF_MANIFESTS} and {shallow:.3f}s with 1, "
        f"which is not the expected speedup: with a {SLEEP_PER_MANIFEST_SECONDS}s delay injected "
        f"into every manifest read, prefetching all the manifests should leave about one delay on "
        f"the critical path instead of roughly {NUMBER_OF_MANIFESTS // 2}"
    )
