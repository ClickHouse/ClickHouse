-- Regression test for the follow-up to issue #96452 (PR #96487, optimizeTopK.cpp):
-- the top-K optimization (skip-index / dynamic prewhere filtering) must NOT be applied
-- when `arrayJoin` is lifted above `Sort` between `Limit` and `Sorting`.
--
-- `tryExecuteFunctionsAfterSorting` lifts expressions that do not depend on the sort
-- columns above `Sort`, turning the plan into `Limit -> Expression -> Sorting`. PR #96487
-- taught `tryOptimizeTopK` to walk through such lifted `ExpressionStep`s so the optimization
-- still applies to the `ALIAS` shape. But `arrayJoin` changes the number of rows: reading
-- only the top-K source rows by the sort key can discard source rows that are still needed
-- to produce `LIMIT` rows after the expansion. This is especially visible when the first
-- rows by the sort key have empty arrays and therefore produce no output rows of their own.
-- The walk-through must therefore bail out on `arrayJoin`, consistent with the same check
-- in `optimizeLazyMaterialization2`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_topk_array_join SYNC;
CREATE TABLE test_topk_array_join
(
    ord UInt32,
    arr Array(UInt32)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 100;

-- The first 100 rows by `ord` (the smallest values) have EMPTY arrays, so they produce no
-- output rows. `ord` is not the table sort key, so the query needs a full `Sort` with the
-- top-K optimization eligible, and `arrayJoin(arr)` does not depend on `ord`, so it is lifted
-- above `Sort` into an `ExpressionStep` between `Limit` and `Sorting`.
INSERT INTO test_topk_array_join SELECT number, if(number < 100, [], range(number, number + 3)) FROM numbers(10000);

-- The top-K dynamic prewhere filter must NOT be present for the `arrayJoin` query.
-- Without the `hasArrayJoin` guard in `tryOptimizeTopK`, the walk-through would add the
-- `__topKFilter` here (this assertion would print a non-zero count).
SELECT 'arrayJoin_no_topk_filter';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT arrayJoin(arr) AS e FROM test_topk_array_join ORDER BY ord LIMIT 10
      SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

DROP TABLE IF EXISTS test_topk_array_join SYNC;
