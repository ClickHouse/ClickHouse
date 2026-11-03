-- Tags: distributed

-- A shard's partial aggregation in order, with GROUP BY keys longer than the sorted prefix, must not run the
-- GROUP BY top-K heap: it skipped whole blocks of keys and made AggregatingInOrderTransform abort.

DROP TABLE IF EXISTS t_top_k_in_order;
CREATE TABLE t_top_k_in_order (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_top_k_in_order SELECT number, number FROM numbers(10000);

SET optimize_aggregation_in_order = 1, optimize_read_in_order = 1, max_threads = 1, max_block_size = 1000;
SET enable_group_by_top_k_optimization = 1, group_by_top_k_optimization_shared_boundary = 1, enable_group_by_top_k_dynamic_filtering = 0;
SET query_plan_max_limit_for_top_k_optimization = 0, aggregation_in_order_max_block_bytes = 50000000;
-- The CI profile sets max_rows_to_group_by, which disables the shard-side top-K pushdown.
SET max_rows_to_group_by = 0;

SELECT a FROM remote('127.0.0.{1,2}', currentDatabase(), t_top_k_in_order) GROUP BY a, 'x' ORDER BY a LIMIT 5;
SELECT a, b FROM remote('127.0.0.{1,2}', currentDatabase(), t_top_k_in_order) GROUP BY a, b ORDER BY a LIMIT 5;
SELECT a FROM remote('127.0.0.{1,2}', currentDatabase(), t_top_k_in_order) GROUP BY a, 'x' ORDER BY a LIMIT 5
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;

-- The heap is dropped from the in-order shard aggregation, and still runs in the hash one (positive control).
SELECT a FROM remote('127.0.0.{1,2}', currentDatabase(), t_top_k_in_order) GROUP BY a, 'x' ORDER BY a LIMIT 5
SETTINGS prefer_localhost_replica = 0, log_comment = '05355_in_order';
SELECT a FROM remote('127.0.0.{1,2}', currentDatabase(), t_top_k_in_order) GROUP BY a, 'x' ORDER BY a LIMIT 5
SETTINGS optimize_aggregation_in_order = 0, prefer_localhost_replica = 0, log_comment = '05355_hash';

SYSTEM FLUSH LOGS query_log;

SELECT
    countIf(NOT is_initial_query) AS shard_queries,
    sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0 AS skipped
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_date >= yesterday()
    AND initial_query_id =
    (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '05355_in_order'
            AND is_initial_query
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

SELECT
    countIf(NOT is_initial_query) AS shard_queries,
    sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0 AS skipped
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_date >= yesterday()
    AND initial_query_id =
    (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '05355_hash'
            AND is_initial_query
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    );

DROP TABLE t_top_k_in_order;
