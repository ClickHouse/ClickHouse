-- Verify use_top_k_dynamic_filtering_for_variable_length_types gates the dynamic
-- filter for variable-length sort columns (String, Array, Map, ...). The dynamic
-- filter inserts a __topKFilter Prewhere; check whether it appears in the plan.

DROP TABLE IF EXISTS t_topk_string;

CREATE TABLE t_topk_string (id Int64, s String, n UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64;

-- Distribution similar to MobilePhoneModel: lex-min ('') dominates, where the
-- dynamic-filter prewhere costs more than it saves.
INSERT INTO t_topk_string SELECT number, if(number % 20 = 0, concat('m', toString(number)), ''), number FROM numbers(2000);

-- ===== String column =====

-- `query_plan_max_limit_for_top_k_optimization` is randomized in the flaky
-- check; pin it explicitly so a small randomized value (e.g. 1) does not
-- disable the entire top-k optimization for our `LIMIT 5` queries.

-- 1. Default (variable-length gate OFF): no __topKFilter prewhere expected.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT s FROM t_topk_string ORDER BY s LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 2. Variable-length gate ON: __topKFilter prewhere is used.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT s FROM t_topk_string ORDER BY s LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 3. Master setting OFF: no __topKFilter even if variable-length gate is ON.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT s FROM t_topk_string ORDER BY s LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 0, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- ===== Fixed-length column =====

-- 4. Int column: dynamic filter is unaffected by the new setting.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT n FROM t_topk_string ORDER BY n LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT n FROM t_topk_string ORDER BY n LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- ===== Nullable of fixed-length is treated as fixed-length =====

DROP TABLE IF EXISTS t_topk_nullable;
CREATE TABLE t_topk_nullable (id Int64, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_topk_nullable SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(2000);

-- 5. Nullable(UInt32) is fixed-length per haveMaximumSizeOfValue, so the gate
--    does not block dynamic filtering here. The optimization may still be
--    rejected at a later stage (the executeGeneral path handles Nullable),
--    but the planner-level gate must not exclude it.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT n FROM t_topk_nullable ORDER BY n LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- ===== End-to-end correctness =====

-- 6. Sanity: results are identical regardless of the new setting.
SELECT
    (SELECT groupArray(s) FROM (SELECT s FROM t_topk_string ORDER BY s LIMIT 5
        SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, query_plan_max_limit_for_top_k_optimization = 100))
    =
    (SELECT groupArray(s) FROM (SELECT s FROM t_topk_string ORDER BY s LIMIT 5
        SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100));

DROP TABLE t_topk_string;
DROP TABLE t_topk_nullable;
