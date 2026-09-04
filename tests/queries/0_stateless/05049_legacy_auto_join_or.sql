-- Legacy analyzer must accept ON ... OR ... when join_algorithm = auto, because auto
-- falls back to HashJoin (same as the planner path).

-- left (0,10) matches right (0,99) on k; left (1,11) matches right (2,11) on v
SELECT a.k, a.v, b.k, b.v
FROM
(
    SELECT 0 AS k, 10 AS v
    UNION ALL
    SELECT 1, 11
) AS a
INNER JOIN
(
    SELECT 0 AS k, 99 AS v
    UNION ALL
    SELECT 2, 11
) AS b
ON a.k = b.k OR a.v = b.v
ORDER BY a.k, a.v, b.k, b.v
SETTINGS enable_analyzer = 0, join_algorithm = 'auto', query_plan_join_swap_table = 0;

SELECT a.k, a.v, b.k, b.v
FROM
(
    SELECT 0 AS k, 10 AS v
    UNION ALL
    SELECT 1, 11
) AS a
INNER JOIN
(
    SELECT 0 AS k, 99 AS v
    UNION ALL
    SELECT 2, 11
) AS b
ON a.k = b.k OR a.v = b.v
ORDER BY a.k, a.v, b.k, b.v
SETTINGS enable_analyzer = 0, join_algorithm = 'partial_merge'; -- { serverError NOT_IMPLEMENTED }
