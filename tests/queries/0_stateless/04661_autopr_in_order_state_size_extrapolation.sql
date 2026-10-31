-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: to stabilize the collected statistics

-- The automatic parallel replicas statistics collector sizes the aggregate-state columns produced by
-- `AggregatingInOrderTransform` from their serialized states, because `byteSize` does not see the shared
-- arena the states live in. Serializing states is not cheap, so only the sampled blocks do it and the rest
-- are extrapolated. This test drives that path with arena-backed `min(String)` states through a small
-- `aggregation_in_order_max_block_bytes`, which splits the result into many small blocks, and checks the
-- extrapolated output-bytes estimate against the same query fed by large blocks: both must land on the
-- same value up to sampling error.

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET optimize_aggregation_in_order=1;

-- External aggregation is not supported by the statistics collector
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- For runs with the old analyzer
SET enable_analyzer=1;

-- Single-stream in-order aggregation (AggregatingInOrderTransform path)
SET max_threads=1;

DROP TABLE IF EXISTS t_small_chunks;
DROP TABLE IF EXISTS t_large_chunks;

-- Two identical tables, so that the two queries get different plan hashes and both collect statistics.
CREATE TABLE t_small_chunks(key UInt64, s String) ENGINE=MergeTree ORDER BY key
    SETTINGS index_granularity=8192, auto_statistics_types='';
CREATE TABLE t_large_chunks(key UInt64, s String) ENGINE=MergeTree ORDER BY key
    SETTINGS index_granularity=8192, auto_statistics_types='';

INSERT INTO t_small_chunks SELECT number, concat('value_', leftPad(toString(number), 10, '0'), repeat('x', number % 64)) FROM numbers(2e5);
INSERT INTO t_large_chunks SELECT * FROM t_small_chunks;

SELECT key, min(s) FROM t_small_chunks GROUP BY key FORMAT Null
    SETTINGS log_comment='04661_agg_in_order_small_chunks', aggregation_in_order_max_block_bytes=8192;

SELECT key, min(s) FROM t_large_chunks GROUP BY key FORMAT Null
    SETTINGS log_comment='04661_agg_in_order_large_chunks';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Both queries must have collected statistics, and the estimates must agree within 2x.
SELECT
    countIf(output_bytes > 0) AS queries_with_statistics,
    maxIf(output_bytes, small) < 2 * minIf(output_bytes, NOT small) AS small_chunks_estimate_not_inflated,
    maxIf(output_bytes, NOT small) < 2 * minIf(output_bytes, small) AS small_chunks_estimate_not_deflated
FROM (
    SELECT
        log_comment LIKE '%small%' AS small,
        ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE '04661_agg_in_order_%chunks') AND (type = 'QueryFinish')
)
FORMAT TSVWithNames;

DROP TABLE t_small_chunks;
DROP TABLE t_large_chunks;
