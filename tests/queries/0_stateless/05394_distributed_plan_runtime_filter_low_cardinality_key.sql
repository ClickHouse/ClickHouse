-- Tags: no-old-analyzer

-- A transported runtime filter on a `LowCardinality(String)` join key: the partial filters are
-- serialized, merged and applied without changing the join result.

CREATE TABLE big (bid LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid LowCardinality(String)) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT toString(number), number FROM numbers(10000);
INSERT INTO small SELECT toString(number * 5) FROM numbers(100);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;
SET distributed_plan_join_runtime_filters = 1;

SELECT count() FROM big, small WHERE bid = sid SETTINGS log_comment = '05394_low_cardinality_key';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;

-- At least one build task made its filter state for the exchange.
SELECT sum(ProfileEvents['RuntimeFilterStatesSent']) >= 1
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query LIKE 'stage_%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = '05394_low_cardinality_key');
