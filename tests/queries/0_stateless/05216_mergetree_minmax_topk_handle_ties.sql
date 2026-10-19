-- Tests for MergeTreeIndexBulkGranulesMinMax::getTopKMarks with handle_ties=true.
--
-- The top-K skip-index optimisation uses a minmax skip index to pre-filter data
-- granules before sorting. MergeTreeDataSelectExecutor sets handle_ties=true when
-- either of two conditions holds (MergeTreeDataSelectExecutor.cpp:1112):
--   (a) the ORDER BY has more than one column (num_sort_columns > 1), or
--   (b) the skip index has GRANULARITY > 1.
-- With handle_ties=true the global getTopKMarks<true> (MergeTreeIndexMinMax.cpp
-- lines 457-475) keeps all granules that tie on the boundary value instead of
-- stopping after exactly n granules, so the subsequent sort-pass never misses
-- rows that sit precisely at the top-k threshold.
--
-- Previously uncovered: global getTopKMarks<true> result-extraction block
-- (MergeTreeIndexMinMax.cpp lines 457-475): the for-loop that extracts
-- n*index_granularity granules and the while-loop that extends selection for ties.
-- The existing long-tagged test (03711) covers the same paths but is excluded
-- from the nightly coverage run.
--
-- Tags: no-parallel-replicas
-- no-parallel-replicas: top-K skip-index optimisation is disabled for
-- parallel replicas (tryOptimizeTopK returns 0 when make_distributed_plan=true)

-- === Scenario 1: single-column ORDER BY with GRANULARITY 2 ===
-- index.granularity = 2 > 1  -->  top_k_handle_ties = true
-- All values = 1 ensures every index granule has min = max = 1, forcing
-- the tie-extension while-loop (lines 392-396) to fire.

DROP TABLE IF EXISTS t_topk_handle_ties;
CREATE TABLE t_topk_handle_ties
(
    id UInt32,
    v  UInt32,
    INDEX vix v TYPE minmax GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8,
         min_bytes_for_wide_part = 0,
         index_granularity_bytes = 0;

-- 200 rows -> 25 data marks -> 13 index entries -> 25 granule entries.
-- With LIMIT 5 and GRANULARITY 2: min_granules_to_select = 5*2 = 10 < 25,
-- so the early-return (n >= granules.size()) is NOT taken.
-- All v=1 means every granule min=max=1, so after extracting 10 granules
-- in the for-loop, the while-loop appends the remaining 15 tied granules.
INSERT INTO t_topk_handle_ties SELECT number, 1 FROM numbers(200);

-- Verify the optimization fires (EXPLAIN shows "Filter TopK Granules").
SELECT trimLeft(explain) AS explain
FROM (
    EXPLAIN indexes=1
    SELECT v FROM t_topk_handle_ties
    ORDER BY v ASC LIMIT 5
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0
)
WHERE explain LIKE '%TopK%';

-- Correctness: must return 5 rows all equal to 1.
SELECT v FROM t_topk_handle_ties
ORDER BY v ASC LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0;

-- DESC direction also exercises handle_ties (uses max values from index).
SELECT v FROM t_topk_handle_ties
ORDER BY v DESC LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0;

-- Observability of tie extension (review feedback): the correct top-5 by
-- (v ASC, id DESC) lives in the LAST tied granules (ids 199..195). If a
-- regression kept only the first n*index_granularity granules, the result
-- would be 79..75 instead — the queries above could not see that.
SELECT id FROM t_topk_handle_ties
ORDER BY v ASC, id DESC LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0;

SELECT id FROM t_topk_handle_ties
ORDER BY v DESC, id DESC LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0;

DROP TABLE t_topk_handle_ties;

-- === Scenario 2: multi-column ORDER BY (num_sort_columns > 1) with GRANULARITY 1 ===
-- sort_description.size() > 1  -->  top_k_handle_ties = true even with GRANULARITY 1.
-- Exercises the same handle_ties=true code paths via a different trigger condition.

DROP TABLE IF EXISTS t_topk_multi_col;
CREATE TABLE t_topk_multi_col
(
    id UInt32,
    v  UInt32,
    w  UInt32,
    INDEX vix v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8,
         min_bytes_for_wide_part = 0,
         index_granularity_bytes = 0;

INSERT INTO t_topk_multi_col SELECT number, number % 5, number % 3 FROM numbers(200);

-- multi-column ORDER BY: v ASC, w ASC  -->  num_sort_columns = 2 > 1
SELECT trimLeft(explain) AS explain
FROM (
    EXPLAIN indexes=1
    SELECT v, w FROM t_topk_multi_col
    ORDER BY v ASC, w ASC LIMIT 5
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0
)
WHERE explain LIKE '%TopK%';

SELECT v, w FROM t_topk_multi_col
ORDER BY v ASC, w ASC LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0;

-- Same observability for the multi-column trigger: correct answer requires
-- the highest ids among rows tied on (v, w) = (0, 0), which span granules
-- far beyond the first n selected.
SELECT id FROM t_topk_multi_col
ORDER BY v ASC, w ASC, id DESC LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes = 1, query_plan_max_limit_for_top_k_optimization = 0, use_top_k_dynamic_filtering = 0;

DROP TABLE t_topk_multi_col;
