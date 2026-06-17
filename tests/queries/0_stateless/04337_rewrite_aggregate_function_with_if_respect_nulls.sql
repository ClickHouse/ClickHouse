-- Tests that optimize_rewrite_aggregate_function_with_if does not fire for
-- aggregate functions that preserve NULL payload (the *_respect_nulls family).
-- Rewriting f(if(cond, x, NULL)) -> fIf(x, cond) drops the NULL-payload rows,
-- which is only valid when f skips NULL rows. respect_nulls functions count
-- those rows, so the rewrite silently changed results (issue #107421).

SET enable_analyzer = 1;
SET optimize_rewrite_aggregate_function_with_if = 1;

-- { echoOn }

-- respect_nulls functions keep the NULL row: the first/last value is NULL.
-- With the optimization on these must still return \N (not the rewritten value).
SELECT anyRespectNulls(if(number % 2 = 0, NULL, number)) FROM numbers(4);
SELECT first_value_respect_nulls(if(number % 2 = 0, NULL, number)) FROM numbers(4);
SELECT anyRespectNulls(if(number % 2 = 1, number, NULL)) FROM numbers(4);
SELECT anyLast_respect_nulls(if(number < 3, number, NULL)) FROM numbers(4);
SELECT last_value_respect_nulls(if(number < 3, number, NULL)) FROM numbers(4);

-- A combinator wrapping a respect_nulls function (e.g. -OrNull / -OrDefault) must also be
-- detected: the NULL-preserving signal lives on the wrapped leaf, not on the combinator.
SELECT first_value_respect_nullsOrNull(if(number % 2 = 0, NULL, number)) FROM numbers(4);
SELECT anyRespectNullsOrNull(if(number % 2 = 1, number, NULL)) FROM numbers(4);
SELECT anyLast_respect_nullsOrNull(if(number < 3, number, NULL)) FROM numbers(4);
SELECT first_value_respect_nullsOrDefault(if(number % 2 = 0, NULL, number)) FROM numbers(4);

-- Variant/Dynamic carry NULL via a discriminator (not Nullable), so canContainNull is true while
-- makeNullableSafe leaves the type unchanged. The first row is the NULL payload and must be kept.
SELECT anyRespectNulls(if(number = 0, NULL, number::Variant(String, UInt64))) FROM numbers(4);
SELECT anyRespectNulls(if(number = 0, NULL, number::Dynamic)) FROM numbers(4);

-- Even NULL-skipping aggregates (count/any) must NOT be rewritten when the if result is Variant/Dynamic:
-- the NULL branch becomes a discriminator-NULL payload row that the aggregate still processes, so the
-- -If form (which skips it) changes the result (count goes 4 -> 3, any goes \N -> 1).
SELECT count(if(number = 0, NULL, number::Variant(String, UInt64))) FROM numbers(4);
SELECT count(if(number = 0, NULL, number::Dynamic)) FROM numbers(4);
SELECT any(if(number = 0, NULL, number::Variant(String, UInt64))) FROM numbers(4);
SELECT any(if(number = 0, NULL, number::Dynamic)) FROM numbers(4);
SELECT sum(countSubstrings(explain, 'function_name: countIf')) = 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(if(number = 0, NULL, number::Variant(String, UInt64))) FROM numbers(4));

-- respect_nulls aggregates are NOT rewritten to the -If form (the function name stays).
SELECT sum(countSubstrings(explain, 'function_name: any_respect_nulls')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT anyRespectNulls(if(number % 2 = 0, NULL, number)) FROM numbers(4));
SELECT sum(countSubstrings(explain, 'function_name: anyLast_respect_nulls')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT anyLast_respect_nulls(if(number < 3, number, NULL)) FROM numbers(4));
SELECT sum(countSubstrings(explain, 'function_name: any_respect_nullsOrNull')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT first_value_respect_nullsOrNull(if(number % 2 = 0, NULL, number)) FROM numbers(4));

-- NULL-skipping aggregates ARE still rewritten to the -If form (no regression).
SELECT sum(countSubstrings(explain, 'function_name: anyIf')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT any(if(number % 2 = 0, NULL, number)) FROM numbers(4));
SELECT sum(countSubstrings(explain, 'function_name: countIf')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(if(number > 1, number, NULL)) FROM numbers(4));
SELECT sum(countSubstrings(explain, 'function_name: sumIf')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sum(if(number > 1, number, NULL)) FROM numbers(4));
SELECT sum(countSubstrings(explain, 'function_name: avgIf')) > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT avg(if(number > 1, number, NULL)) FROM numbers(4));

-- Results of the still-optimized functions are unchanged.
SELECT count(if(number > 1, number, NULL)), sum(if(number > 1, number, NULL)), avg(if(number > 1, number, NULL)) FROM numbers(4);

-- Zero-argument count is reachable here: NormalizeCountVariantsPass rewrites count(1)/sum(1)
-- to a zero-argument count, whose null adapter indexes argument_types[0]. The optimization
-- must not consult the adapter before the single-if-argument shape is known (must not crash).
SELECT count() FROM numbers(4);
SELECT count(1) FROM numbers(4);
SELECT sum(1) FROM numbers(4);

-- count(if(...)) where the if result is NOT Nullable resolves to AggregateFunctionCount, whose
-- null adapter (AggregateFunctionCountNotNullUnary) rejects a non-Nullable argument. The
-- null-preserving probe must use a Nullable argument so this does not crash (issue surfaced on
-- 01710_minmax_count_projection: count(if(d=4, d, 1)) under force_optimize_projection).
-- force_optimize_projection = 1 needs an applicable projection, so pin the projection settings
-- the runner randomizes off (optimize_use_projections/optimize_use_implicit_projections/
-- optimize_trivial_count_query) or it throws PROJECTION_NOT_USED instead of exercising the pass.
DROP TABLE IF EXISTS t_04337;
CREATE TABLE t_04337 (d Int64) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_04337 SELECT number FROM numbers(1000);
SELECT count(if(d = 4, d, 1)) FROM t_04337 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1;
DROP TABLE t_04337;

-- { echoOff }
