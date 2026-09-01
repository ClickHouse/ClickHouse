-- The rewrite runs before `optimizeLazyFinal`, which only picks up a `WHERE` filter that sits directly
-- above the reading step. Splicing the unpack `ExpressionStep` in between would hide the filter and turn
-- a filtered `FINAL` query on `ReplacingMergeTree` into a full `FINAL` read.

DROP TABLE IF EXISTS row_wrapper_lazy_final;

SET enable_analyzer = 1;
SET allow_experimental_row_type = 1;
CREATE TABLE row_wrapper_lazy_final
(
    a UInt64,
    b UInt64,
    c UInt64,
    v UInt64,
    w Row(b UInt64, c UInt64) MATERIALIZED tuple(b, c)
)
ENGINE = ReplacingMergeTree(v) ORDER BY a;

SYSTEM STOP MERGES row_wrapper_lazy_final;
INSERT INTO row_wrapper_lazy_final (a, b, c, v) SELECT number, number % 7, number % 11, 1 FROM numbers(10000);
INSERT INTO row_wrapper_lazy_final (a, b, c, v) SELECT number, number % 5, number % 13, 2 FROM numbers(5000, 10000);

-- The filtered FINAL query still takes the lazy FINAL path.
SELECT countIf(explain LIKE '%InputSelector%') > 0 FROM (
    EXPLAIN actions = 0 SELECT a, b, c FROM row_wrapper_lazy_final FINAL WHERE b = 1
        SETTINGS query_plan_use_row_wrappers = 1, query_plan_optimize_lazy_final = 1,
                 max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0,
                 optimize_move_to_prewhere = 0, enable_parallel_replicas = 0
);

-- Without lazy FINAL the same query is served from the wrapper.
SELECT countIf(explain LIKE '%w Row(b UInt64, c UInt64)%') > 0 FROM (
    EXPLAIN header = 1 SELECT a, b, c FROM row_wrapper_lazy_final FINAL WHERE b = 1
        SETTINGS query_plan_use_row_wrappers = 1, query_plan_optimize_lazy_final = 0,
                 optimize_move_to_prewhere = 0, enable_parallel_replicas = 0
);

-- So is the filtered query without FINAL.
SELECT countIf(explain LIKE '%w Row(b UInt64, c UInt64)%') > 0 FROM (
    EXPLAIN header = 1 SELECT a, b, c FROM row_wrapper_lazy_final WHERE b = 1
        SETTINGS query_plan_use_row_wrappers = 1, query_plan_optimize_lazy_final = 1,
                 max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0,
                 optimize_move_to_prewhere = 0, enable_parallel_replicas = 0
);

SELECT count(), sum(b), sum(c) FROM row_wrapper_lazy_final FINAL WHERE b = 1
    SETTINGS query_plan_use_row_wrappers = 1, query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0, optimize_move_to_prewhere = 0;
SELECT count(), sum(b), sum(c) FROM row_wrapper_lazy_final FINAL WHERE b = 1
    SETTINGS query_plan_use_row_wrappers = 0, query_plan_optimize_lazy_final = 0;

DROP TABLE row_wrapper_lazy_final;
