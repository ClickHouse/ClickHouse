-- Tags: no-old-analyzer, no-flaky-check
-- no-old-analyzer: distributed planning requires the analyzer.
-- no-flaky-check: the distributed-plan queries take tens of seconds on a TSan build, and the
-- flaky check runs all new tests at once, so the runs at peak load exceed its per-run time limit.

-- The column-less "any" scatter must spread rows across its destination buckets instead of
-- funneling everything into bucket 0. Verified through `processors_profile_log`: more than one
-- partial-aggregation instance consumes a substantial share of the rows (empty buckets still
-- deliver a single service row, hence the threshold). `max_threads = 1` keeps one instance per task.

DROP TABLE IF EXISTS t_any_scatter;
CREATE TABLE t_any_scatter (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_any_scatter;
-- Several parts so every scatter source emits several chunks
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(1000, 1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(2000, 1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(3000, 1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(4000, 1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(5000, 1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(6000, 1000);
INSERT INTO t_any_scatter SELECT number % 50, number FROM numbers(7000, 1000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET max_threads = 1;
SET log_processors_profiles = 1;

SELECT '-- 1. single-source scatter under global aggregation';
EXPLAIN SELECT sum(v) FROM t_any_scatter SETTINGS distributed_plan_default_shuffle_join_bucket_count = 4;
SELECT sum(v) FROM t_any_scatter
  SETTINGS distributed_plan_default_shuffle_join_bucket_count = 4, log_comment = '04505_scatter_single_source';

SELECT '-- 2. scatter into a join stage over an expression join key';
SELECT count() FROM t_any_scatter AS a, t_any_scatter AS b WHERE a.k = (b.k + 1) % 50
  SETTINGS distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 2,
           log_comment = '04505_scatter_multi_source';

-- The log introspection below is not the subject of the test; a distributed read of the
-- constantly merging system log tables can fail on parts replaced after planning.
SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- The `event_time` bound keeps the log scans cheap: without it every flaky-check rerun scans all
-- the log rows accumulated by the earlier runs and the test can exceed the per-run time limit.
SELECT 'single source spreads:', countIf(input_rows >= 100) >= 2
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND name = 'AggregatingTransform' AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '04505_scatter_single_source' AND type = 'QueryFinish');

SELECT 'multi source spreads:', countIf(input_rows >= 100) >= 2
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND name = 'AggregatingTransform' AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '04505_scatter_multi_source' AND type = 'QueryFinish');

DROP TABLE t_any_scatter;
