"""
Functional coverage for `iceberg_parallel_manifest_decode_threads`.

The setting causes `IcebergIterator` to spawn N background producer threads that
share an atomic counter for the snapshot's `manifest_list_entries`. The producers
also share a `filter_dag` (cloned shallowly), so a query whose `WHERE` clause has a
literal-tuple `IN` set exercises concurrent lazy materialization of that set
(`FutureSetFromTuple`), which must be thread-safe.

This test:
1. Creates an iceberg table with 8 single-row inserts, each into its own
   partition under `PARTITIONED BY (identity(id))`. Each Spark insert produces
   one data manifest, so the snapshot's `manifest_list_entries` size is 8.
2. Runs the same query at `iceberg_parallel_manifest_decode_threads in {1, 4, 16}`
   through `icebergS3Cluster` and asserts identical row counts/content across all N.
3. Repeats for a query with `WHERE id IN (literal_tuples)` to exercise the
   concurrently-materialized IN set; the result must match the serial run (a
   regression here would surface as a silent zero-row return at high N).
4. Tags every query with a unique `query_id` and looks up its
   `ProfileEvents['IcebergParallelManifestDecodeThreadsSpawned']` directly in
   `system.query_log` (no LIKE pattern matching on query text, no sums across
   queries). Asserts each per-query bump equals `min(threads, total_manifests)`
   when N > 1, and 0 when N == 1.
"""

import uuid

import pytest

from helpers.iceberg_utils import (
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str,
)


# Each Spark insert into a partitioned iceberg table produces one data manifest;
# the test inserts into 8 distinct partitions, so the snapshot's manifest list has
# exactly 8 data entries. The implementation clamps `parallel_threads` to
# `min(requested, max(total_manifests, 1))`, so any N >= 8 will yield an effective
# 8-thread spawn (and a corresponding 8-bump on the ProfileEvent counter).
TOTAL_MANIFESTS = 8


def _execute_spark(spark, cluster, storage_type, table, query):
    return execute_spark_query_general(spark, cluster, storage_type, table, query)


def _expected_events(threads):
    """Mirror of `resolveParallelManifestDecodeThreads`: counter bumps by 0 in
    serial mode, otherwise by `min(threads, total_manifests)`."""
    if threads <= 1:
        return 0
    return min(threads, TOTAL_MANIFESTS)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_parallel_manifest_decode_matches_serial(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_parallel_manifest_decode_" + storage_type + "_" + get_uuid_str()

    _execute_spark(
        spark,
        started_cluster_iceberg_with_spark,
        storage_type,
        table_name,
        f"""
            CREATE TABLE {table_name} (
                id INT,
                value STRING
            )
            USING iceberg
            PARTITIONED BY (identity(id))
            OPTIONS('format-version'='2')
        """,
    )

    for i in range(TOTAL_MANIFESTS):
        _execute_spark(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            table_name,
            f"INSERT INTO {table_name} VALUES ({i}, 'v{i}')",
        )

    cluster_expr = get_creation_expression(
        storage_type,
        table_name,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )

    # Tag each query with a unique query_id so we can pick its row out of
    # system.query_log by exact match — no LIKE on query text, no sums across
    # concurrent test runs.
    run_tag = uuid.uuid4().hex

    def query_with_threads(label, select, threads):
        qid = f"parallel_decode_{run_tag}_{label}_{threads}"
        result = instance.query(
            f"{select} SETTINGS iceberg_parallel_manifest_decode_threads = {threads}",
            query_id=qid,
        )
        return qid, result

    # Plain SELECT — establishes baseline parity across N.
    select_all = f"SELECT id, value FROM {cluster_expr} ORDER BY id"
    serial_qid, serial = query_with_threads("plain", select_all, 1)
    parallel_4_qid, parallel_4 = query_with_threads("plain", select_all, 4)
    parallel_16_qid, parallel_16 = query_with_threads("plain", select_all, 16)
    assert serial == parallel_4
    assert serial == parallel_16

    # SELECT with a literal-tuple IN filter — the parallel producers share the
    # filter DAG, so this exercises concurrent lazy materialization of the IN set.
    # The result must match the serial run (a regression would silently return 0
    # rows at high N).
    select_in = (
        f"SELECT id, value FROM {cluster_expr} "
        f"WHERE id IN ((1), (3), (5), (7)) "
        f"ORDER BY id"
    )
    serial_in_qid, serial_in = query_with_threads("in", select_in, 1)
    parallel_in_4_qid, parallel_in_4 = query_with_threads("in", select_in, 4)
    parallel_in_16_qid, parallel_in_16 = query_with_threads("in", select_in, 16)
    assert serial_in != "", "baseline (threads=1) returned no rows; fixture broken"
    assert serial_in == parallel_in_4
    assert serial_in == parallel_in_16

    # ProfileEvent observability: per-query exact lookup by query_id.
    instance.query("SYSTEM FLUSH LOGS")

    def events_for_qid(qid):
        result = instance.query(
            f"""
            SELECT ProfileEvents['IcebergParallelManifestDecodeThreadsSpawned']
            FROM system.query_log
            WHERE query_id = '{qid}' AND type = 'QueryFinish'
            """
        ).strip()
        assert result, f"no QueryFinish row in system.query_log for query_id={qid}"
        return int(result)

    cases = [
        (serial_qid, 1),
        (parallel_4_qid, 4),
        (parallel_16_qid, 16),
        (serial_in_qid, 1),
        (parallel_in_4_qid, 4),
        (parallel_in_16_qid, 16),
    ]
    for qid, threads in cases:
        actual = events_for_qid(qid)
        expected = _expected_events(threads)
        assert actual == expected, (
            f"query_id={qid} (threads={threads}): "
            f"expected IcebergParallelManifestDecodeThreadsSpawned = {expected}, "
            f"got {actual}"
        )
