drop table if exists logs;
CREATE TABLE logs
(
    `a` int,
    `b` int,
    `c` int,
    INDEX i c TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
AS SELECT
    number,
    number,
    number
FROM numbers(1);

SELECT a,b,c
FROM logs
WHERE a > 0
ORDER BY c DESC
LIMIT 1
SETTINGS use_top_k_dynamic_filtering = true, query_plan_max_limit_for_top_k_optimization=1, query_plan_optimize_lazy_materialization=1, query_plan_max_limit_for_lazy_materialization=100;
