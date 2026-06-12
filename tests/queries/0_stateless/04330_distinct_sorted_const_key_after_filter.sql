DROP TABLE IF EXISTS distinct_sorted_const_key_after_filter;

CREATE TABLE distinct_sorted_const_key_after_filter
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY (a, b);

INSERT INTO distinct_sorted_const_key_after_filter
SELECT number % 10, if(number % 2 = 0, 7, 8)
FROM numbers(100);

SELECT count()
FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT a, b
    FROM distinct_sorted_const_key_after_filter
    WHERE b = 7
    ORDER BY a
    SETTINGS optimize_distinct_in_order = 1,
        optimize_move_to_prewhere = 0,
        query_plan_optimize_prewhere = 0,
        query_plan_optimize_lazy_materialization = 0,
        query_plan_remove_unused_columns = 0
)
WHERE explain LIKE '%DistinctSortedTransform%';

SELECT DISTINCT a, b
FROM distinct_sorted_const_key_after_filter
WHERE b = 7
ORDER BY a
SETTINGS optimize_distinct_in_order = 1,
    optimize_move_to_prewhere = 0,
    query_plan_optimize_prewhere = 0,
    query_plan_optimize_lazy_materialization = 0,
    query_plan_remove_unused_columns = 0;

DROP TABLE distinct_sorted_const_key_after_filter;
