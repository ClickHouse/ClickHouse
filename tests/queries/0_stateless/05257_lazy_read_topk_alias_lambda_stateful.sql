-- Lazy materialization replays expressions lifted between `Limit` and `Sort` after the real `LIMIT`,
-- and top-K filtering lets such expressions see only the top-K source rows. Both optimizations must
-- refuse lifted expressions with a row-sensitive function also when that function is inside a
-- lambda body (`canReplayAfterLimit` / `canObserveOnlyTopKRows`).
--
-- `query_plan_push_down_limit` would move the `Limit` below the lifted expression, so the expression
-- would run after the `LIMIT` with or without the optimizations, and the guards would never be
-- reached. Disable it to keep the `Limit -> Expression [lifted up part] -> Sorting` shape, and assert
-- that shape explicitly.

SET enable_analyzer = 1, max_threads = 1, enable_parallel_replicas = 0, query_plan_push_down_limit = 0;

DROP TABLE IF EXISTS test_lambda_stateful SYNC;
CREATE TABLE test_lambda_stateful
(
    ord        UInt32,
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short'),
    INDEX ord_minmax(ord) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 100;

INSERT INTO test_lambda_stateful SELECT number, repeat('x', number % 20) FROM numbers(10000);

-- 1. The lambda is lifted above `Sorting` and stays below `Limit`. `rand(x)` depends on the lambda
--    argument, so it stays in the lambda body instead of being computed outside and captured.
SELECT 'lifted_shape';
SELECT position(arrayStringConcat(groupArray(if(s LIKE '%[lifted up part]%', 'Expression [lifted up part]', splitByChar(' ', s)[1])), ' -> '),
                'Limit -> Expression [lifted up part] -> Sorting') > 0
FROM
(
    SELECT trimLeft(explain) AS s
    FROM (EXPLAIN pretty = 0, compact = 0
          SELECT arrayMap(x -> rand(x), range(3)) AS n, body_alias AS s FROM test_lambda_stateful ORDER BY ord LIMIT 10 OFFSET 5
          SETTINGS query_plan_optimize_lazy_materialization = 0, use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0)
    WHERE match(s, '^(Expression|Limit|Sorting|ReadFromMergeTree) \\(')
);

-- 2. Control: a lifted lambda without row-sensitive functions in its body keeps both optimizations,
--    so the refusals below are caused by the body of the lambda.
SELECT 'pure_lambda_lazy';
SELECT count() > 0
FROM (EXPLAIN actions = 1
      SELECT arrayMap(x -> x + 1, range(3)) AS n, body_alias AS s FROM test_lambda_stateful ORDER BY ord LIMIT 10 OFFSET 5
      SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 100,
               use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0)
WHERE explain LIKE '%LazilyRead%';

SELECT 'pure_lambda_topk';
SELECT count() > 0
FROM (EXPLAIN actions = 1
      SELECT arrayMap(x -> x + 1, range(3)) AS n, body_alias AS s FROM test_lambda_stateful ORDER BY ord LIMIT 10
      SETTINGS query_plan_optimize_lazy_materialization = 0,
               use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

-- 3. A function that is not deterministic in the scope of a query inside the lambda body disables both.
SELECT 'row_sensitive_lambda_lazy';
SELECT count()
FROM (EXPLAIN actions = 1
      SELECT arrayMap(x -> rand(x), range(3)) AS n, body_alias AS s FROM test_lambda_stateful ORDER BY ord LIMIT 10 OFFSET 5
      SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 100,
               use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0)
WHERE explain LIKE '%LazilyRead%';

SELECT 'row_sensitive_lambda_topk';
SELECT count()
FROM (EXPLAIN actions = 1
      SELECT arrayMap(x -> rand(x), range(3)) AS n, body_alias AS s FROM test_lambda_stateful ORDER BY ord LIMIT 10
      SETTINGS query_plan_optimize_lazy_materialization = 0,
               use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

-- 4. The results with a stateful function in a lifted lambda match the results without the optimizations.
DROP TABLE IF EXISTS test_lambda_stateful_on SYNC;
DROP TABLE IF EXISTS test_lambda_stateful_off SYNC;
CREATE TABLE test_lambda_stateful_on (n Array(UInt64), s String) ENGINE = Memory;
CREATE TABLE test_lambda_stateful_off (n Array(UInt64), s String) ENGINE = Memory;

-- 4a. Lazy materialization with `OFFSET`.
INSERT INTO test_lambda_stateful_on
SELECT arrayMap(x -> rowNumberInAllBlocks(), range(1)) AS n, body_alias AS s
FROM test_lambda_stateful ORDER BY ord DESC LIMIT 10 OFFSET 5
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 100,
         use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

INSERT INTO test_lambda_stateful_off
SELECT arrayMap(x -> rowNumberInAllBlocks(), range(1)) AS n, body_alias AS s
FROM test_lambda_stateful ORDER BY ord DESC LIMIT 10 OFFSET 5
SETTINGS query_plan_optimize_lazy_materialization = 0,
         use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

SELECT 'lazy_lambda_result_matches';
SELECT (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_lambda_stateful_on ORDER BY n, s))
     = (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_lambda_stateful_off ORDER BY n, s));

TRUNCATE TABLE test_lambda_stateful_on;
TRUNCATE TABLE test_lambda_stateful_off;

-- 4b. Top-K filtering.
INSERT INTO test_lambda_stateful_on
SELECT arrayMap(x -> rowNumberInAllBlocks(), range(1)) AS n, body_alias AS s
FROM test_lambda_stateful ORDER BY ord LIMIT 10
SETTINGS query_plan_optimize_lazy_materialization = 0,
         use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000;

INSERT INTO test_lambda_stateful_off
SELECT arrayMap(x -> rowNumberInAllBlocks(), range(1)) AS n, body_alias AS s
FROM test_lambda_stateful ORDER BY ord LIMIT 10
SETTINGS query_plan_optimize_lazy_materialization = 0,
         use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0, query_plan_max_limit_for_top_k_optimization = 0;

SELECT 'topk_lambda_result_matches';
SELECT (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_lambda_stateful_on ORDER BY n, s))
     = (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_lambda_stateful_off ORDER BY n, s));

DROP TABLE test_lambda_stateful_on SYNC;
DROP TABLE test_lambda_stateful_off SYNC;
DROP TABLE test_lambda_stateful SYNC;
