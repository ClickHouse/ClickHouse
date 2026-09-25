-- Lazy materialization replays expressions lifted between `Limit` and `Sort` after the real `LIMIT`,
-- and top-K filtering lets such expressions see only the top-K source rows. Both optimizations must
-- refuse lifted expressions with a stateful function also when that function is hidden inside a
-- lambda body, which a plain scan over the `ActionsDAG` nodes does not reach.

SET enable_analyzer = 1, max_threads = 1, enable_parallel_replicas = 0;

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

DROP TABLE IF EXISTS test_lambda_stateful_on SYNC;
DROP TABLE IF EXISTS test_lambda_stateful_off SYNC;
CREATE TABLE test_lambda_stateful_on (n Array(UInt64), s String) ENGINE = Memory;
CREATE TABLE test_lambda_stateful_off (n Array(UInt64), s String) ENGINE = Memory;

-- 1. Lazy materialization with `OFFSET`.
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

-- 2. Top-K filtering.
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
