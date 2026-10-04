-- Regression test for the top-K walk-through added in PR #96487 (`optimizeTopK.cpp`).
--
-- `tryExecuteFunctionsAfterSorting` lifts expressions that do not depend on the sort
-- columns above `Sort`, giving a `Limit -> Expression -> Sorting` plan; the PR taught
-- `tryOptimizeTopK` to walk through those lifted `ExpressionStep`s so the `ALIAS` shape
-- keeps skip-index / dynamic top-K filtering. Top-K filtering skips source rows below the
-- sort, so a lifted expression must not be able to observe which rows were skipped:
-- stateful functions (`rowNumberInAllBlocks`, `blockNumber`) and functions that are only
-- deterministic in the scope of a query (`nowInBlock`) are evaluated per block over the
-- stream that reaches them. The walk-through therefore bails out on such expressions,
-- mirroring `canReplayAfterLimit` in `optimizeLazyMaterialization.cpp`.
--
-- The absolute values of the row numbers depend on how the rows are distributed over
-- blocks and replicas, so the assertions compare the result with top-K enabled against the
-- same query with top-K disabled instead of pinning literal values.

SET enable_analyzer = 1, max_threads = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS test_topk_stateful SYNC;
CREATE TABLE test_topk_stateful
(
    ord        UInt32,
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short'),
    severity   LowCardinality(String),
    INDEX ord_minmax(ord) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 100;

INSERT INTO test_topk_stateful
SELECT number, repeat('x', number % 20), if(number % 2 == 0, 'info', 'medium') FROM numbers(10000);

-- 1. Baseline: the plain `ALIAS` shape (nothing row-sensitive is lifted) still gets the
--    dynamic top-K filter, so the guard below does not disable the optimization wholesale.
SELECT 'alias_topk_filter';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT body_alias FROM test_topk_stateful ORDER BY ord LIMIT 10
      SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

-- 2. A stateful function alongside the `ALIAS` column must produce the same result with and
--    without top-K filtering.
DROP TABLE IF EXISTS test_topk_stateful_on SYNC;
DROP TABLE IF EXISTS test_topk_stateful_off SYNC;
CREATE TABLE test_topk_stateful_on (n UInt64, s String) ENGINE = Memory;
CREATE TABLE test_topk_stateful_off (n UInt64, s String) ENGINE = Memory;

INSERT INTO test_topk_stateful_on
SELECT rowNumberInAllBlocks() AS n, body_alias AS s FROM test_topk_stateful ORDER BY ord LIMIT 10
SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000;

INSERT INTO test_topk_stateful_off
SELECT rowNumberInAllBlocks() AS n, body_alias AS s FROM test_topk_stateful ORDER BY ord LIMIT 10
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0, query_plan_max_limit_for_top_k_optimization = 0;

SELECT 'stateful_result_matches_without_topk';
SELECT (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_topk_stateful_on ORDER BY n, s))
     = (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_topk_stateful_off ORDER BY n, s));

-- 3. The same for the filtered shape of issue #96452 (`WHERE ... ORDER BY ... LIMIT`), where
--    the skip-index top-K path can additionally drop whole granules during the read.
TRUNCATE TABLE test_topk_stateful_on;
TRUNCATE TABLE test_topk_stateful_off;

INSERT INTO test_topk_stateful_on
SELECT rowNumberInAllBlocks() AS n, body_alias AS s FROM test_topk_stateful WHERE severity = 'medium' ORDER BY ord LIMIT 10
SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1,
         query_plan_max_limit_for_top_k_optimization = 1000, max_rows_to_read = 0;

INSERT INTO test_topk_stateful_off
SELECT rowNumberInAllBlocks() AS n, body_alias AS s FROM test_topk_stateful WHERE severity = 'medium' ORDER BY ord LIMIT 10
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0, use_skip_indexes_on_data_read = 0,
         query_plan_max_limit_for_top_k_optimization = 0, max_rows_to_read = 0;

SELECT 'filtered_stateful_result_matches_without_topk';
SELECT (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_topk_stateful_on ORDER BY n, s))
     = (SELECT groupArray(t) FROM (SELECT (n, s) AS t FROM test_topk_stateful_off ORDER BY n, s));

DROP TABLE test_topk_stateful_on SYNC;
DROP TABLE test_topk_stateful_off SYNC;
DROP TABLE test_topk_stateful SYNC;
