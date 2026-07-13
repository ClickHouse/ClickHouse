-- Regression test: for read-in-order queries, the stream count is determined
-- by the number of parts, not by data size. When max_streams_to_max_threads_ratio
-- is greater than 1, requested_num_streams exceeds the actual number of parts,
-- which previously set read_stream_count_was_reduced and prevented AggregatingStep
-- from expanding the pipeline back to max_threads after merge-sort.

DROP TABLE IF EXISTS t_read_in_order_agg;

CREATE TABLE t_read_in_order_agg (a UInt32) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES t_read_in_order_agg;

INSERT INTO t_read_in_order_agg SELECT number FROM numbers_mt(1000000);
INSERT INTO t_read_in_order_agg SELECT number FROM numbers_mt(1000000);

-- Single-partition table with two parts: after merge-sort there is 1 stream.
-- With max_streams_to_max_threads_ratio > 1 and read-in-order, the pipeline
-- must still expand to more than 1 stream after aggregation.
-- The new analyzer produces "Resize 1 -> 8" (max_threads); the old analyzer
-- produces "Resize 1 -> 16" (max_threads * ratio), so match both.
SELECT count() > 0
FROM viewExplain('EXPLAIN PIPELINE', '', (
    SELECT a FROM t_read_in_order_agg GROUP BY a
    SETTINGS optimize_aggregation_in_order = 1,
             read_in_order_two_level_merge_threshold = 1e12,
             max_threads = 8,
             max_streams_to_max_threads_ratio = 2
))
WHERE match(explain, 'Resize 1 → ([2-9]|[1-9][0-9]+)');

DROP TABLE t_read_in_order_agg;
