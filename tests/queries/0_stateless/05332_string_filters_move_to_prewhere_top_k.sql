-- A substring search condition that uses all queried columns is moved to PREWHERE with
-- `apply_string_filters_during_scan` only if the reader can apply it as a string filter during the scan.
-- It cannot, when the TopN dynamic filter `__topKFilter` will be added to PREWHERE on the same column.

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, optimize_prewhere_after_pushdown = 1;
SET apply_string_filters_during_scan = 1;
SET use_columns_cache = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;

DROP TABLE IF EXISTS t_string_filter_top_k;

CREATE TABLE t_string_filter_top_k (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_top_k SELECT number, 'value ' || toString(number) FROM numbers(1000);

SELECT 'without TopN dynamic filter';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_top_k WHERE s LIKE '%needle%' ORDER BY s LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 0);

SELECT 'with TopN dynamic filter';
SELECT countIf(explain LIKE '%Prewhere filter column%__topKFilter%') > 0, countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_top_k WHERE s LIKE '%needle%' ORDER BY s LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 1);

SELECT s FROM t_string_filter_top_k WHERE s LIKE '%99%' ORDER BY s LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_string_filter_top_k;
