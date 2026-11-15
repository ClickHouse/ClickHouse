-- Verify use_top_k_dynamic_filtering_for_variable_length_types gates the dynamic
-- filter for variable-length sort columns (String, Array, Map, ...). The dynamic
-- filter inserts a __topKFilter Prewhere; check whether it appears in the plan.

DROP TABLE IF EXISTS t_topk_string;

CREATE TABLE t_topk_string (id Int64, s String, n UInt32, a Array(String), lcn LowCardinality(Nullable(String)), lcs LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64;

-- Distribution similar to MobilePhoneModel: lex-min ('') dominates, where the
-- dynamic-filter prewhere costs more than it saves.
INSERT INTO t_topk_string SELECT number, if(number % 20 = 0, concat('m', toString(number)), '') AS s, number, [s], if(number % 3 = 0, NULL, s), s
FROM numbers(2000);

-- ===== String column =====

-- `query_plan_max_limit_for_top_k_optimization` is randomized in the flaky
-- check; pin it explicitly so a small randomized value (e.g. 1) does not
-- disable the entire top-k optimization for our `LIMIT 5` queries.

-- 1. Variable-length gates OFF: no __topKFilter prewhere expected.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT s FROM t_topk_string ORDER BY s LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 0, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 1a. A single `String` sort column uses the filter with `use_top_k_dynamic_filtering_for_single_string_sort_column`.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT s FROM t_topk_string ORDER BY s LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 1b. More sort columns still need `use_top_k_dynamic_filtering_for_variable_length_types`.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT s, n FROM t_topk_string ORDER BY s, n LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 1c. Not for a column that is not a `String`.
DROP TABLE IF EXISTS t_topk_json;
CREATE TABLE t_topk_json (id Int64, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_topk_json SELECT number, toJSONString(map('a', number)) FROM numbers(2000);
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT j FROM t_topk_json ORDER BY j LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';
DROP TABLE t_topk_json;

-- 1d. Not for `Array(String)`.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT a FROM t_topk_string ORDER BY a LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 1e. Not for `LowCardinality(Nullable(String))`.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT lcn FROM t_topk_string ORDER BY lcn LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

-- 1f. Not for `LowCardinality(String)`.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT lcs FROM t_topk_string ORDER BY lcs LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100
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
