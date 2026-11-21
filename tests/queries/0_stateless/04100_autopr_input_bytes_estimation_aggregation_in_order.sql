-- Tags: no-random-merge-tree-settings, long
-- no-random-merge-tree-settings: to stabilize the test

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET optimize_aggregation_in_order=1;

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

SET max_block_size=65409;

-- For runs with the old analyzer
SET enable_analyzer=1;

DROP TABLE IF EXISTS t_agg_in_order;

CREATE TABLE t_agg_in_order(key UInt64, value UInt64, s String)
ENGINE=MergeTree ORDER BY key
SETTINGS index_granularity=8192, auto_statistics_types='';

INSERT INTO t_agg_in_order SELECT number, number, toString(number) FROM numbers(2e6);

-- Single-stream in-order aggregation (AggregatingInOrderTransform path)
SELECT key, sum(value) FROM t_agg_in_order GROUP BY key FORMAT Null
    SETTINGS log_comment='agg_in_order_single', max_threads=1;

-- Multi-stream in-order aggregation (FinishAggregatingInOrder + MergingAggregatedBucket path)
SELECT key, sum(value) FROM t_agg_in_order GROUP BY key FORMAT Null
    SETTINGS log_comment='agg_in_order_multi', max_threads=4;

-- In-order aggregation with filter
SELECT key, sum(value) FROM t_agg_in_order WHERE key < 1000000 GROUP BY key FORMAT Null
    SETTINGS log_comment='agg_in_order_filter', max_threads=4;

-- In-order aggregation with multiple aggregate functions
SELECT key, sum(value), min(s), count() FROM t_agg_in_order GROUP BY key FORMAT Null
    SETTINGS log_comment='agg_in_order_multi_agg', max_threads=1;

-- group_by_key path: GROUP BY has more columns than the table's ORDER BY prefix.
-- This triggers a different code path in AggregatingInOrderTransform where the sort prefix
-- is shorter than the full GROUP BY, and the output is produced via prepareChunkAndFillSingleLevel.
SELECT key, sum(value) FROM t_agg_in_order WHERE key % 10000 < 1000 GROUP BY key, value FORMAT Null
    SETTINGS log_comment='agg_in_order_group_by_key', max_threads=4;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Check input bytes estimation accuracy (ratio should be within 2x)
SELECT format('{}: input estimation off by {}x (compressed={}, estimated={})', log_comment, round(ratio, 2), compressed_bytes, statistics_input_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] AS compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS statistics_input_bytes,
        greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) AS ratio
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE 'agg_in_order_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE ratio > 2;

-- Check output bytes estimation accuracy against known-good values (ratio should be within 2x).
-- Expected output bytes were measured with default settings on 2e6 rows:
-- execute queries with parallel replicas and with local plan disabled, then take the network received bytes metric as estimation.
SELECT format('{}: output estimation off by {}x (expected~{}, estimated={})', log_comment, round(ratio, 2), expected, statistics_output_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS statistics_output_bytes,
        multiIf(
            log_comment = 'agg_in_order_single', 25519057,
            log_comment = 'agg_in_order_multi', 25515684,
            log_comment = 'agg_in_order_filter', 10096176,
            log_comment = 'agg_in_order_multi_agg', 33649632,
            log_comment = 'agg_in_order_group_by_key', 2532395,
            0) AS expected,
        greatest(expected, statistics_output_bytes) / least(expected, statistics_output_bytes) AS ratio
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE 'agg_in_order_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE ratio > 2;

DROP TABLE t_agg_in_order;
