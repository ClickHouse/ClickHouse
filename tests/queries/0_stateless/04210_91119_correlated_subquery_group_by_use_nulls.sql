-- https://github.com/ClickHouse/ClickHouse/issues/91119
--
-- A correlated subquery references an outer column that becomes `Nullable` after
-- the outer `GROUP BY` with `WITH ROLLUP`/`CUBE`/`GROUPING SETS` and `group_by_use_nulls = 1`.
-- The analyzer used to leave the correlated column reference at its non-Nullable type,
-- so the planner built CAST wrappers and aggregate functions for the original signature.
-- At header inference time the actual input column was already `Nullable`, which produced
-- "Bad cast from type ColumnNullable to ColumnVector<...>" exceptions.
--
-- The fix updates the analyzer so a correlated column reference picks up the Nullable
-- type from the outer scope's `nullable_group_by_keys` set before the planner runs.
--
-- ClickHouse currently rejects correlated subqueries in ORDER BY directly, so each query
-- below either does not need ordering or sorts by a GROUP BY key that is now Nullable.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET group_by_use_nulls = 1;

SELECT '-- ROLLUP ---';
SELECT (SELECT c0)
FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- CUBE ---';
SELECT (SELECT c0)
FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH CUBE
ORDER BY c0 NULLS LAST;

SELECT '-- GROUPING SETS ---';
SELECT (SELECT c0)
FROM (SELECT 1::Bool) t0(c0)
GROUP BY GROUPING SETS ((c0), ())
ORDER BY c0 NULLS LAST;

SELECT '-- numeric ---';
SELECT (SELECT c0)
FROM (SELECT 1) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- complex expression ---';
SELECT (SELECT c0 + 1)
FROM (SELECT toInt32(1)) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- modulo ---';
SELECT (SELECT c0 % 2)
FROM (SELECT toInt32(3)) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- multiple GROUP BY keys ---';
SELECT number, (SELECT number % 2), sum(number)
FROM numbers(3)
GROUP BY number
WITH ROLLUP
ORDER BY number NULLS LAST;

SELECT '-- aggregate over correlated column ---';
SELECT number, (SELECT sum(number))
FROM numbers(3)
GROUP BY number
WITH ROLLUP
ORDER BY number NULLS LAST;

-- Multiple GROUP BY keys with CUBE and an aggregate over the correlated key (STID 2672-39ba).
-- CUBE makes both `number` and `number % 2` Nullable in the extra rows; the correlated
-- `(SELECT sum(number))` references the outer `number` key. `sum(number)` (a plain
-- aggregate, not the correlated subquery) is used in ORDER BY for a deterministic order.
SELECT '-- multi-key CUBE with aggregate over correlated column ---';
SELECT number, number % 2, (SELECT sum(number))
FROM numbers(10)
GROUP BY number % 2, number WITH CUBE
ORDER BY number, number % 2, sum(number) NULLS LAST;

SELECT '-- HAVING with correlated subquery ---';
SELECT number, sum(number) AS s
FROM numbers(3)
GROUP BY number
WITH ROLLUP
HAVING s >= (SELECT number)
ORDER BY number NULLS LAST;

SELECT '-- WHERE with correlated subquery ---';
SELECT number, sum(number) AS s
FROM numbers(3)
WHERE number >= (SELECT number - 1)
GROUP BY number
WITH ROLLUP
ORDER BY number NULLS LAST;

SELECT '-- Nullable source ---';
SELECT (SELECT c0)
FROM (SELECT toNullable(1::Bool)) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

-- Aggregate over a correlated String key: exercises the ColumnNullable -> ColumnString wrapper
-- rather than the numeric ColumnVector path.
SELECT '-- min over correlated String ---';
SELECT (SELECT min(c0))
FROM (SELECT 'a'::String) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

SELECT '-- minOrDefault over correlated String with GROUPING SETS ---';
SELECT (SELECT minOrDefault(c0))
FROM (SELECT 'a'::String) t0(c0)
GROUP BY GROUPING SETS ((c0), ())
ORDER BY c0 NULLS LAST;

-- Sanity check: a non-correlated subquery in the same context must NOT have its inner
-- column wrapped as Nullable just because the outer query has `group_by_use_nulls + ROLLUP`.
-- The two `c0` columns happen to share a name; the inner one resolves to its own table
-- expression and stays a plain `String`.
SELECT '-- non-correlated subquery ---';
SELECT (SELECT c0 FROM (SELECT 'inner'::String) t1(c0))
FROM (SELECT 1::Bool) t0(c0)
GROUP BY c0 WITH ROLLUP
ORDER BY c0 NULLS LAST;

-- An inner query's own GROUP BY key sharing the outer rollup key's name AND type must not be
-- wrapped Nullable by the outer scope: nullable_group_by_keys matches on name+type only (column
-- source ignored), but the inner non-rollup query never registers keys, so its local `c` (2) is
-- returned unchanged while only the outer `c` rolls up. Conversely the correlated outer `c`,
-- when projected via an aggregate, still picks up the outer post-rollup Nullable.
SELECT '-- inner local key collides with outer rollup key (name+type) ---';
SELECT t0.c AS oc, (SELECT t1.c FROM (SELECT toInt32(2) AS c) t1 GROUP BY t1.c)
FROM (SELECT toInt32(7) AS c) t0
GROUP BY t0.c WITH ROLLUP
ORDER BY t0.c NULLS LAST;

SELECT '-- correlated outer key under aggregate, inner has same-named local key ---';
SELECT t0.c AS oc, (SELECT any(t0.c) FROM (SELECT toInt32(2) AS c) t1 GROUP BY t1.c)
FROM (SELECT toInt32(7) AS c) t0
GROUP BY t0.c WITH ROLLUP
ORDER BY t0.c NULLS LAST;

SELECT '-- type pin: inner local key stays non-Nullable despite outer rollup key collision ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT t0.c, (SELECT t1.c FROM (SELECT toInt32(2) AS c) t1 GROUP BY t1.c)
    FROM (SELECT toInt32(7) AS c) t0 GROUP BY t0.c WITH ROLLUP)
WHERE explain ILIKE '%column_name: c,%result_type%';

-- A constant LowCardinality GROUP BY key (STID 3721-419e). ConstantNode::convertToNullable used
-- plain makeNullableSafe, which is a no-op on LowCardinality(T) (it cannot be inside Nullable), so
-- the key stayed non-Nullable while its rollup/grouping-sets rows were NULL at runtime, and an
-- aggregate reading the subquery's projected type bad-cast ColumnNullable to ColumnString.
SELECT '-- minSimpleState over LowCardinality constant key ---';
SELECT minSimpleState(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((1)));

SELECT '-- projected type is LowCardinality(Nullable) ---';
SELECT toTypeName(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((1)));

SELECT '-- min over LowCardinality constant key with ROLLUP ---';
SELECT min(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY ROLLUP(1));

SELECT '-- max over LowCardinality constant key with GROUPING SETS ---';
SELECT max(*) FROM (SELECT toLowCardinality('a') GROUP BY GROUPING SETS ((1)));

SELECT '-- original fuzzer query ---';
SELECT minDistinct(c0 >= (SELECT minSimpleState(*) FROM (SELECT DISTINCT toLowCardinality('1') GROUP BY GROUPING SETS ((1)))))
FROM (SELECT 1 AS c0 LIMIT 197) AS t0;

-- Contributed by @coderashed (the exact AST-fuzzer seed query from his duplicate PR #108714, which
-- he closed in favour of this one): a correlated scalar subquery `toString(number)` inside a String
-- function, over a `GROUPING SETS` key. Aborts on master with `Bad cast ColumnNullable to ColumnString`.
SELECT '-- correlated toString in concat over GROUPING SETS ---';
SELECT x FROM (
    SELECT concat('s', (SELECT toString(number))) AS x
    FROM numbers(5) GROUP BY GROUPING SETS ((number))
) ORDER BY x;

-- Contributed by @coderashed: a type-pinning regression check. The other cases above check query
-- results, which still pass if a future change silently stops wrapping the correlated key as
-- Nullable (the values are unchanged, only the declared type is wrong). These assert the analyzer
-- types directly via EXPLAIN QUERY TREE, so a regression that drops the Nullable wrapping fails with
-- a clean diff. `id`s are masked because they are not stable across runs.
SELECT '-- type pin: bare correlated column is Nullable, outer key is not ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT (SELECT c) FROM (SELECT toInt32(3) AS c) t GROUP BY c WITH ROLLUP)
WHERE explain ILIKE '%column_name: c,%result_type%';

SELECT '-- type pin: compound correlated key (modulo) is Nullable ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT (SELECT c % 2) FROM (SELECT toInt32(3) AS c) t GROUP BY c % 2 WITH ROLLUP)
WHERE explain ILIKE '%function_name: modulo%result_type%';

SELECT '-- type pin: aggregate argument stays non-Nullable ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH ROLLUP)
WHERE explain ILIKE '%column_name: number,%result_type%';

-- Statistics aggregates over a correlated key (skewSamp/kurtPop/varSamp/covarSamp/...).
-- These share one add() that reads the argument via an UNCHECKED static_cast
-- (AggregateFunctionStatisticsSimple.h), unlike min/sum/argMax above which assert_cast and
-- raise a clean "Bad cast" exception when handed the wrong column type. So the same
-- non-Nullable-argument bug here is a silent heap-buffer-overflow (memory-safety) on release
-- builds instead of an exception: on master these abort with
-- "AddressSanitizer: heap-buffer-overflow ... in AggregateFunctionVarianceSimple::add".
-- numbers(64) is used so the +1 out-of-bounds read lands on an ASan redzone rather than the
-- PaddedPODArray SIMD padding. Each must run to completion and produce a correct result.
SELECT '-- skewSamp over correlated key, CUBE ---';
SELECT k, (SELECT skewSamp(a.k))
FROM (SELECT number AS k FROM numbers(64)) AS a
GROUP BY k WITH CUBE
ORDER BY k NULLS LAST FORMAT Null;

SELECT '-- varSamp over correlated key, ROLLUP ---';
SELECT k, (SELECT varSamp(a.k))
FROM (SELECT number AS k FROM numbers(64)) AS a
GROUP BY k WITH ROLLUP
ORDER BY k NULLS LAST FORMAT Null;

SELECT '-- kurtPop over correlated key, GROUPING SETS ---';
SELECT k, (SELECT kurtPop(a.k))
FROM (SELECT number AS k FROM numbers(64)) AS a
GROUP BY GROUPING SETS ((k), ())
ORDER BY k NULLS LAST FORMAT Null;

SELECT '-- covarSamp (two-argument statistics aggregate) over correlated key, CUBE ---';
SELECT k, (SELECT covarSamp(a.k, a.k))
FROM (SELECT number AS k FROM numbers(64)) AS a
GROUP BY k WITH CUBE
ORDER BY k NULLS LAST FORMAT Null;

-- Small deterministic result check for the statistics path (single-element groups -> nan;
-- the CUBE super-aggregate null-key row is \N).
SELECT '-- skewSamp over correlated key, CUBE, values ---';
SELECT k, (SELECT skewSamp(a.k))
FROM (SELECT number AS k FROM numbers(8)) AS a
GROUP BY k WITH CUBE
ORDER BY k NULLS LAST;

-- Type pin: the correlated statistics-aggregate argument column picks up the outer
-- rollup key's Nullable type (so the aggregate resolves with the Null combinator instead
-- of a bare function reading a ColumnNullable). On master the argument stays Int32.
SELECT '-- type pin: correlated statistics-aggregate argument is Nullable ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT (SELECT skewSamp(a.k)) FROM (SELECT toInt32(3) AS k) AS a GROUP BY k WITH CUBE)
WHERE explain ILIKE '%column_name: k,%result_type%';

-- https://github.com/ClickHouse/ClickHouse/issues/106377
-- The correlated subquery returns a tuple whose second element is the outer rollup key.
-- The tuple function node kept the non-Nullable element type, so after decorrelation made the
-- input Nullable the planner raised "Unexpected return type from tuple. Expected Tuple(UInt8,
-- UInt64). Got Tuple(UInt8, Nullable(UInt64))" (and the same on EXPLAIN). The subquery column
-- reference must pick up the outer key's Nullable type so the tuple resolves Nullable from the start.
SELECT '-- 106377 correlated tuple, ROLLUP ---';
SELECT (SELECT (1, number))
FROM numbers(1)
GROUP BY number WITH ROLLUP
ORDER BY number NULLS LAST;

SELECT '-- 106377 correlated tuple, CUBE ---';
SELECT (SELECT (1, number))
FROM numbers(2)
GROUP BY number WITH CUBE
ORDER BY number NULLS LAST;

SELECT '-- type pin: correlated tuple result is Nullable(Tuple(...)) ---';
SELECT replaceRegexpAll(trim(explain), 'id: [0-9]+', 'id: N') AS line
FROM (EXPLAIN QUERY TREE
    SELECT (SELECT (1, number)) FROM numbers(1) GROUP BY number WITH ROLLUP)
WHERE explain ILIKE '%column_name: number,%result_type%';
