-- Verify that tryOptimizeTopK (which runs after optimizePrewhere) correctly
-- handles the interaction between __topKFilter and existing WHERE clauses
-- pushed to prewhere.
-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS topk_where_test;

CREATE TABLE topk_where_test
(
    id UInt64,
    val UInt32,
    cat UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO topk_where_test SELECT number, number % 10000, number % 100 FROM numbers(100000);

SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 100;

-- Case 1: No WHERE — __topKFilter should be injected as prewhere.
SELECT 'case1_topk_present';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions=1
    SELECT val FROM topk_where_test ORDER BY val LIMIT 5
    SETTINGS enable_analyzer=1
) WHERE explain LIKE '%topKFilter%';

-- Case 2: WHERE on a different column (cat) — optimizePrewhere pushes it to
-- prewhere, so __topKFilter should NOT be injected.
SELECT 'case2_topk_absent';
SELECT count() == 0 FROM (
    EXPLAIN PLAN actions=1
    SELECT val FROM topk_where_test WHERE cat = 5 ORDER BY val LIMIT 5
    SETTINGS enable_analyzer=1
) WHERE explain LIKE '%topKFilter%';

SELECT 'case2_where_in_prewhere';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions=1
    SELECT val FROM topk_where_test WHERE cat = 5 ORDER BY val LIMIT 5
    SETTINGS enable_analyzer=1
) WHERE explain LIKE '%Prewhere filter column: equals%';

-- Case 3: Correctness — results must match regardless of the setting.
SELECT 'case3_no_where';
SELECT val FROM topk_where_test ORDER BY val LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT val FROM topk_where_test ORDER BY val LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1;

SELECT 'case3_with_where';
SELECT val FROM topk_where_test WHERE cat = 5 ORDER BY val LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT val FROM topk_where_test WHERE cat = 5 ORDER BY val LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1;

SELECT 'case3_desc';
SELECT val FROM topk_where_test ORDER BY val DESC LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 0;

SELECT val FROM topk_where_test ORDER BY val DESC LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE topk_where_test;
