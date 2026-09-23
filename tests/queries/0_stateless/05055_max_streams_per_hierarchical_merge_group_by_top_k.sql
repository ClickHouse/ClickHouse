SET optimize_trivial_group_by_limit_query = 0;
SET serialize_query_plan = 0;
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;

SELECT '-- the synthesized full sort honors the invalid value --';
SELECT k
FROM (SELECT number % 100 AS k FROM numbers_mt(1000))
GROUP BY k
LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1, max_streams_per_hierarchical_merge = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- valid values still execute --';
SELECT count()
FROM
(
    SELECT k
    FROM (SELECT number % 100 AS k FROM numbers_mt(1000))
    GROUP BY k
    LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 1, max_streams_per_hierarchical_merge = 0
);

SELECT count()
FROM
(
    SELECT k
    FROM (SELECT number % 100 AS k FROM numbers_mt(1000))
    GROUP BY k
    LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 1, max_streams_per_hierarchical_merge = 16
);
