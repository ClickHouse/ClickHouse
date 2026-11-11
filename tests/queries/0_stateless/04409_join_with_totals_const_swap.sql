-- The TOTALS row of a `WITH TOTALS`-aggregated subquery must keep the constant
-- values coming from the other side of a JOIN, regardless of build/probe order.
-- Previously, when the constant side became the probe side (e.g. because of join
-- table swapping, see `query_plan_join_swap_table`, or simply by writing it as
-- the left table), the synthesized default TOTALS row collapsed the constants to
-- type defaults instead of keeping them.
-- Related to 03205_column_type_check under query_plan_optimize_join_order_randomize.

SELECT '-- constant side on the right, no swap';
SELECT *
FROM
(
    SELECT count() AS cnt FROM numbers(4) GROUP BY number % 2 WITH TOTALS
) AS t,
(
    SELECT 42 AS x, 99 AS y
) AS u
ORDER BY cnt
SETTINGS query_plan_join_swap_table = 'false';

SELECT '-- constant side on the right, with swap';
SELECT *
FROM
(
    SELECT count() AS cnt FROM numbers(4) GROUP BY number % 2 WITH TOTALS
) AS t,
(
    SELECT 42 AS x, 99 AS y
) AS u
ORDER BY cnt
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- constant side on the left, no swap';
SELECT *
FROM
(
    SELECT 42 AS x, 99 AS y
) AS u,
(
    SELECT count() AS cnt FROM numbers(4) GROUP BY number % 2 WITH TOTALS
) AS t
ORDER BY cnt
SETTINGS query_plan_join_swap_table = 'false';

SELECT '-- constant side on the left, with swap';
SELECT *
FROM
(
    SELECT 42 AS x, 99 AS y
) AS u,
(
    SELECT count() AS cnt FROM numbers(4) GROUP BY number % 2 WITH TOTALS
) AS t
ORDER BY cnt
SETTINGS query_plan_join_swap_table = 'true';
