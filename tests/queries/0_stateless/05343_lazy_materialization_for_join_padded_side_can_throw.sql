-- Lazy materialization over joins may compute a value of the side a join can leave unmatched above the LIMIT, as
-- `if(mask, value, default)`, which evaluates it on the rows the join padded as well, where its arguments hold the
-- defaults. So only a function that cannot throw is computed that way. A comparison is cheap but can throw: decimals of
-- different scales are rescaled to a common scale, which can overflow. Such a comparison is computed below the LIMIT,
-- where the join pads its result, while a comparison of decimals of the same scale is computed above it.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, d2 Decimal(18, 2), e2 Decimal(18, 2), d6 Decimal(18, 6)) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
-- Only the even keys are in `r`, so the odd ones are left unmatched.
INSERT INTO r SELECT number * 2, number / 4, number / 4, number / 4 FROM numbers(50);

SELECT '-- different scales: computed below the LIMIT';
SELECT l.k, l.heavy, r.eq FROM l LEFT JOIN (SELECT k, d2 = d6 AS eq FROM r) AS r ON l.k = r.k ORDER BY l.k LIMIT 6;
SELECT l.k, l.heavy, r.eq FROM l LEFT JOIN (SELECT k, d2 = d6 AS eq FROM r) AS r ON l.k = r.k ORDER BY l.k LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%Lazily read columns: d2%')
FROM (EXPLAIN SELECT l.k, l.heavy, r.eq FROM l LEFT JOIN (SELECT k, d2 = d6 AS eq FROM r) AS r ON l.k = r.k ORDER BY l.k LIMIT 6);

SELECT '-- the same scale: computed above the LIMIT';
SELECT l.k, l.heavy, r.eq FROM l LEFT JOIN (SELECT k, d2 = e2 AS eq FROM r) AS r ON l.k = r.k ORDER BY l.k LIMIT 6;
SELECT l.k, l.heavy, r.eq FROM l LEFT JOIN (SELECT k, d2 = e2 AS eq FROM r) AS r ON l.k = r.k ORDER BY l.k LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%Lazily read columns: d2%')
FROM (EXPLAIN SELECT l.k, l.heavy, r.eq FROM l LEFT JOIN (SELECT k, d2 = e2 AS eq FROM r) AS r ON l.k = r.k ORDER BY l.k LIMIT 6);

DROP TABLE l;
DROP TABLE r;
