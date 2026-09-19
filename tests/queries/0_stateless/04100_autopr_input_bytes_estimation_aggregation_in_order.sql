-- Tags: no-random-merge-tree-settings, no-random-settings, long, no-flaky-check
-- no-random-merge-tree-settings / no-random-settings: to stabilize the test. The autopr output-bytes
-- estimate serializes the aggregation output with the default codec; under the `ZSTD(3)` default it
-- is very sensitive to block-sizing query settings, so randomized settings make the estimate swing
-- several-fold and the fixed expectations below cannot hold. Fix the query settings to keep it stable.
-- no-flaky-check: this is a `long` test (2e6-row insert + several parallel-replicas queries); re-running
-- it dozens of times in the flaky check exceeds the per-iteration budget in the slow debug build and the
-- run is stopped (signal 20). The estimates are validated by the regular stateless runs across builds.

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

-- Pin the codec to `ZSTD(3)` (the server default) so the actually-read compressed bytes match the
-- input-estimate sample, which is serialized with `getDefaultCodec` (otherwise the `no-random-*`
-- harness would inject `LZ4` and the read bytes would diverge from the `ZSTD(3)`-based estimate).
CREATE TABLE t_agg_in_order(key UInt64, value UInt64, s String)
ENGINE=MergeTree ORDER BY key
SETTINGS index_granularity=8192, auto_statistics_types='', default_compression_codec='ZSTD(3)';

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

-- Check the output bytes estimate against what the replicas actually send (ratio within 2.5x).
-- The expected values are `NetworkReceiveBytes` on the initiator, measured on 2e6 rows with the local
-- plan disabled and compression forced on every replica of the cluster.
--
-- The estimate runs 1.8x to 2.1x high on these shapes and the tolerance covers that. The overshoot is
-- the aggregate states: they are sampled from the hash table and so priced in hash-table order, while
-- the replicas send them in key order, and `sum(value)` over `value = key` is a monotone function of
-- the key - exactly the case where the two orders compress differently. For states that do not vary
-- with the key the estimate matches the wire within a few percent.
SELECT format('{}: output estimation off by {}x (transferred={}, estimated={})', log_comment, round(ratio, 2), expected, statistics_output_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS statistics_output_bytes,
        multiIf(
            log_comment = 'agg_in_order_single', 4150899,
            log_comment = 'agg_in_order_multi', 4146665,
            log_comment = 'agg_in_order_filter', 2111321,
            log_comment = 'agg_in_order_group_by_key', 645369,
            0) AS expected,
        greatest(expected, statistics_output_bytes) / least(expected, statistics_output_bytes) AS ratio
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE 'agg_in_order_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE ratio > 2.5;

DROP TABLE t_agg_in_order;
