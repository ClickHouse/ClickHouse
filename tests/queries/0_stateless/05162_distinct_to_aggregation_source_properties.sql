SET query_plan_convert_distinct_to_aggregation = 1;
SET query_plan_remove_redundant_distinct = 0;
SET enable_parallel_replicas = 0;
SET distinct_overflow_mode = 'throw';
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET max_rows_to_group_by = 0;
SET max_threads = 4;

-- Finite prepared sources use aggregation; unbounded generators keep streaming deduplication.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT x FROM values('x UInt64', 1, 2, 1));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT x FROM generateRandom('x UInt64'));
SELECT DISTINCT x FROM values('x UInt64', 1, 2, 1) ORDER BY x;

-- Recursive sources keep streaming deduplication so a result limit can stop their evaluation.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE
      WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t)
      SELECT DISTINCT n FROM t);
WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t)
SELECT DISTINCT n FROM t
SETTINGS max_threads = 1, max_result_rows = 3, result_overflow_mode = 'break',
    max_recursive_cte_evaluation_depth = 10 FORMAT Null;

-- A `Merge` source can wrap an unbounded generator whose read bounds are unknown to its consumer.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM merge('system', '^numbers$'));
SELECT DISTINCT number FROM merge('system', '^numbers$')
SETTINGS max_threads = 1, max_block_size = 1, max_result_rows = 3,
    result_overflow_mode = 'break', max_rows_to_read = 100 FORMAT Null;

-- The initiator can optimize a complete plan when serialized execution is enabled.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM numbers_mt(1000) SETTINGS serialize_query_plan = 1);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT x FROM values('x UInt64', 1, 2, 1) LIMIT 1);

DROP TABLE IF EXISTS t_distinct_source_properties;
CREATE TABLE t_distinct_source_properties (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_distinct_source_properties SELECT number % 100 FROM numbers(10000);

-- Distributed fragments execute aggregation chosen with the full consumer tree available.
SELECT countIf(explain LIKE '%Aggregating%') > 0
FROM (EXPLAIN SELECT DISTINCT k FROM t_distinct_source_properties
      SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1);
SELECT count(), sum(k) FROM (SELECT DISTINCT k FROM t_distinct_source_properties)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT count(), sum(k), min(c), max(c) FROM (SELECT DISTINCT k, 7 AS c FROM t_distinct_source_properties)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- With one thread there is no preliminary `DISTINCT`; the serialized aggregation enforces both limits.
SELECT DISTINCT k FROM t_distinct_source_properties
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, max_threads = 1, max_rows_in_distinct = 50 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT DISTINCT k FROM t_distinct_source_properties
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, max_threads = 1, max_bytes_in_distinct = 1 FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- A consumer limit keeps `DISTINCT` streaming before the plan is split into fragments.
SELECT count() FROM (SELECT DISTINCT k FROM t_distinct_source_properties LIMIT 3)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

DROP TABLE t_distinct_source_properties;
