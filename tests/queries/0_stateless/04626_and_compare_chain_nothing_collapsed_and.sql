-- The `optimize_and_compare_chain` / `optimize_redundant_comparisons` passes prune and fold an
-- AND-chain of comparisons. Both skip a Nullable-typed AND, because a comparison over a Nullable
-- operand cannot participate in range pruning. That guard checks only the AND's own result type,
-- which is unsound: when one AND operand is `Nothing`-typed, the function resolver collapses the
-- whole AND's result type to bare `Nothing` (`Nothing::isNullable()` is false), so the guard passes
-- even though another operand is a comparison over a directly-Nullable expression. That operand then
-- reached the pruning path and hit `chassert(!raw_type->isNullable())`.

SET enable_analyzer = 1;

-- 1) No logical error. These queries are otherwise invalid (a `Nothing`-typed value cannot be
--    materialized), so they must fail with a normal handled exception, never a logical error.
--    optimize_and_compare_chain path (chassert reached via the seed loop of tryOptimizeAndCompareChain):
SELECT tuple((materialize(toNullable(NULL)) = 1) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_and_compare_chain = 1; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((materialize(toNullable(1::Int32)) = 1) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_and_compare_chain = 1; -- { serverError ILLEGAL_COLUMN }
--    optimize_redundant_comparisons path (tryOptimizeAndCompareNotEqualsChain), independent of the chain setting:
SELECT tuple((materialize(toNullable(1::Int32)) = 1) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((materialize(toNullable(1::Int32)) != 1) AND (assumeNotNull(materialize(toNullable(NULL))) != 2)) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }

-- 2) The optimizer still runs on such a `Nothing`-collapsed AND: the directly-Nullable operand is
--    kept as-is (the new opaque-filter fallback) while a redundant non-null comparison in the SAME
--    AND is still folded. In `x > 3 AND x > 5`, `x > 3` is redundant and pruned, so exactly one
--    `greater` survives when enabled against both when disabled. Pin both counts: a relative
--    comparison also holds when the predicates disappear entirely.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_redundant_comparisons = 1) WHERE explain LIKE '%function_name: greater,%';
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_redundant_comparisons = 0) WHERE explain LIKE '%function_name: greater,%';
-- The exact predicate that used to hit the assertion is `equals(...) -> Nullable(Nothing)`; assert it
-- survives (exactly one such node) rather than being silently dropped. The chain's other `equals`
-- returns `Nothing`, so match the `Nullable(Nothing)` result type specifically.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_redundant_comparisons = 1) WHERE explain LIKE '%function_name: equals, function_type: ordinary, result_type: Nullable(Nothing)%';

-- 3) Correctness guard: a genuine (non-collapsed) AND-compare chain over a Nullable column must still
--    be pruned/folded correctly, i.e. the optimization result matches the unoptimized one, and NULLs
--    are excluded by the comparisons. `optimize_and_compare_chain` / `optimize_redundant_comparisons`
--    must not change results.
DROP TABLE IF EXISTS t_and_chain_nullable;
CREATE TABLE t_and_chain_nullable (x Nullable(Int32)) ENGINE = Memory;
INSERT INTO t_and_chain_nullable VALUES (1), (5), (NULL), (10);
SELECT groupArray(x) FROM (SELECT x FROM t_and_chain_nullable WHERE (x > 3) AND (x > 5) ORDER BY x SETTINGS optimize_and_compare_chain = 1);
SELECT groupArray(x) FROM (SELECT x FROM t_and_chain_nullable WHERE (x > 3) AND (x > 5) ORDER BY x SETTINGS optimize_and_compare_chain = 0);
SELECT groupArray(x) FROM (SELECT x FROM t_and_chain_nullable WHERE (x != 1) AND (x != 5) ORDER BY x SETTINGS optimize_redundant_comparisons = 1);
SELECT groupArray(x) FROM (SELECT x FROM t_and_chain_nullable WHERE (x != 1) AND (x != 5) ORDER BY x SETTINGS optimize_redundant_comparisons = 0);
DROP TABLE t_and_chain_nullable;
-- The checks above compare results, which stay equal even if `optimize_and_compare_chain` stops
-- deriving anything. Pin the derivation itself with exact node counts: `a < b AND b < 5` gains the
-- transitive `a < 5`, so the enabled tree holds exactly 3 `less` nodes against 2 when disabled.
SELECT count() = 3 FROM (EXPLAIN QUERY TREE SELECT a, b FROM values('a Int32, b Int32', (1, 2), (4, 9)) WHERE (a < b) AND (b < 5) SETTINGS optimize_and_compare_chain = 1) WHERE explain ILIKE '%function_name: less,%';
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT a, b FROM values('a Int32, b Int32', (1, 2), (4, 9)) WHERE (a < b) AND (b < 5) SETTINGS optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';

-- 4) In a `Nothing`-collapsed AND, an operand may be a comparison whose constant side is itself
--    NULL-valued (e.g. `expr = NULL`), found by the AST fuzzer mutating case 2's `= 2` to `= NULL`.
--    A NULL-valued constant carries no comparable value, so `tryOptimizeAndCompareNotEqualsChain`
--    must not treat it as the constant side (it used to hit `chassert(!literal->getValue().isNull())`).
--    The whole AND is still `Nothing`-typed, so these queries fail with a normal handled exception.
--    RHS-NULL constant:
SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = NULL)) SETTINGS optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (assumeNotNull(materialize(toNullable(NULL))) != NULL)) SETTINGS optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
--    LHS-NULL constant (the other assertion branch):
SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (NULL = assumeNotNull(materialize(toNullable(NULL))))) SETTINGS optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
--    Independent of the pruning setting (the classification loop runs unconditionally):
SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (assumeNotNull(materialize(toNullable(NULL))) = NULL)) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 0; -- { serverError ILLEGAL_COLUMN }

-- 5) Liveness of the keep-as-is mechanism for the NULL-valued-constant operand: the optimizer must
--    still run on this `Nothing`-collapsed AND (so a redundant sibling is pruned) while keeping the
--    NULL-valued `equals` operands rather than skipping classification entirely. In the tree with
--    pruning on, `x > 3` is redundant given `x > 5`, so exactly one `greater` node survives; with
--    pruning off both survive; both NULL-valued `equals` operands are kept in either case.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = NULL)) SETTINGS optimize_redundant_comparisons = 1) WHERE explain ILIKE '%function_name: greater,%';
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = NULL)) SETTINGS optimize_redundant_comparisons = 0) WHERE explain ILIKE '%function_name: greater,%';
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = NULL)) SETTINGS optimize_redundant_comparisons = 1) WHERE explain ILIKE '%function_name: equals,%';

-- 6) A comparison whose RESULT is nullable must be kept as-is too, even when the raw operand type does
--    not report `isNullable`: `LowCardinality(Nullable(T))` (nested nullability) and the NULL-capable
--    carriers `Dynamic` / `Variant` all yield a nullable comparison result. They used to slip past the
--    guard, fold the contradictory `x = 1 AND x = 2`, and change the handled exception depending on the
--    setting. The error must now be the same regardless of the pruning / chain settings.
SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SELECT tuple((x = 1) AND (x = 2) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x LowCardinality(Nullable(Int32))', NULL) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 0; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((x = 1) AND (x = 2) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x LowCardinality(Nullable(Int32))', NULL) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((x = 1) AND (x = 2) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x Dynamic', NULL) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 0; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((x = 1) AND (x = 2) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x Dynamic', NULL) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((x = 1) AND (x = 2) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x Variant(Int32, String)', NULL) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 0; -- { serverError ILLEGAL_COLUMN }
SELECT tuple((x = 1) AND (x = 2) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x Variant(Int32, String)', NULL) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1; -- { serverError ILLEGAL_COLUMN }
-- Liveness for the LC(Nullable) operand: a redundant NON-nullable sibling (`> 3` given `> 5`) is still
-- pruned to one `greater` node while the LC-nullable `equals` operand is kept, proving the optimizer
-- runs and keeps only that operand opaque rather than declining the whole collapsed AND.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT tuple((x = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x LowCardinality(Nullable(Int32))', NULL) SETTINGS optimize_redundant_comparisons = 1) WHERE explain ILIKE '%function_name: greater,%';
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT tuple((x = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x LowCardinality(Nullable(Int32))', NULL) SETTINGS optimize_redundant_comparisons = 0) WHERE explain ILIKE '%function_name: greater,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT tuple((x = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))))) FROM values('x LowCardinality(Nullable(Int32))', NULL) SETTINGS optimize_redundant_comparisons = 1) WHERE explain ILIKE '%function_name: equals,%';
-- A non-collapsed `LowCardinality(Nullable(T))` chain must still return correct results, unchanged by
-- the optimization (NULL excluded, `1`/`5` filtered out; only `10` remains).
SELECT groupArray(x) FROM (SELECT x FROM values('x LowCardinality(Nullable(Int32))', 1, 5, NULL, 10) WHERE (x != 1) AND (x != 5) ORDER BY x SETTINGS optimize_redundant_comparisons = 1);
SELECT groupArray(x) FROM (SELECT x FROM values('x LowCardinality(Nullable(Int32))', 1, 5, NULL, 10) WHERE (x != 1) AND (x != 5) ORDER BY x SETTINGS optimize_redundant_comparisons = 0);

-- 7) The carrier the AST fuzzer keeps rediscovering on master (`Logical error:
--    '!raw_type->isNullable()'`, STID `2508-50fe`, e.g.
--    https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=d469feea5f342065ffe8b2384d4ddf354dae3978&name_0=MasterCI&name_1=Stress%20test%20%28arm_asan_ubsan%29 ).
--    It reaches the same `addComparisonFilter` through a different route than the cases above: the
--    `Nothing`-typed operand comes from `ARRAY JOIN []` (whose element type is `Nothing`) instead of
--    `assumeNotNull(materialize(toNullable(NULL)))`, and the collapsed AND sits in a `JOIN ON`
--    section. `JOIN ON` is load-bearing - the same AND in a `WHERE` is rejected earlier, so only the
--    join expression lets a `Nothing`-typed AND reach the optimizer.
DROP TABLE IF EXISTS t_and_chain_array_join;
CREATE TABLE t_and_chain_array_join (c0 Int32) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_and_chain_array_join VALUES (1), (2);

--    `t2.c0 = a0` is `Nothing`-typed and collapses the AND's own result type, while
--    `toNullable(t2.c0) > 0` stays `Nullable(UInt8)` and reaches the pruning path. `ARRAY JOIN []`
--    produces no rows, so the query is valid and returns nothing; assert the empty result on every
--    combination of the two settings, since either entry point alone reaches the assertion.
SELECT 1 FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join AS t2 ON (t2.c0 = a0) AND (toNullable(t2.c0) > 0) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1;
SELECT 1 FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join AS t2 ON (t2.c0 = a0) AND (toNullable(t2.c0) > 0) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 0;
SELECT 1 FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join AS t2 ON (t2.c0 = a0) AND (toNullable(t2.c0) > 0) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 1;
SELECT 1 FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join AS t2 ON (t2.c0 = a0) AND (toNullable(t2.c0) > 0) SETTINGS optimize_and_compare_chain = 0, optimize_redundant_comparisons = 0;
--    A result-only check stays green if the queries stop running the optimizer at all, so pin the
--    pruning as well: given `> 5`, the sibling `> 3` is redundant and folded away, leaving the
--    Nullable-result `greater` plus one surviving constant comparison. Both settings are pinned on
--    every query below because `clickhouse-test` randomizes `optimize_and_compare_chain`.
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT 1 FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join AS t2 ON (t2.c0 = a0) AND (toNullable(t2.c0) > 0) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1) WHERE explain ILIKE '%function_name: greater,%';
SELECT count() = 3 FROM (EXPLAIN QUERY TREE SELECT 1 FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join AS t2 ON (t2.c0 = a0) AND (toNullable(t2.c0) > 0) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 0) WHERE explain ILIKE '%function_name: greater,%';
--    The exact fuzzer query: here the Nullable-result comparison is over a correlated scalar
--    subquery rather than a column. `optimize_and_compare_chain` does not gate this one -
--    `tryOptimizeAndCompareChain` skips a chain holding a correlated subquery, while
--    `tryOptimizeAndCompareNotEqualsChain` has no such guard - so it arrives only via
--    `optimize_redundant_comparisons`. A correlated subquery is not supported in a join expression,
--    so the query must report that handled exception instead of aborting.
SELECT 1 AS x FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join ON (t_and_chain_array_join.c0 = a0) AND (t_and_chain_array_join.c0 != a0) AND (0 > (SELECT t_and_chain_array_join.c0)) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 1; -- { serverError NOT_IMPLEMENTED }
SELECT 1 AS x FROM t_and_chain_array_join AS tx ARRAY JOIN [] AS a0 LEFT JOIN t_and_chain_array_join ON (t_and_chain_array_join.c0 = a0) AND (t_and_chain_array_join.c0 != a0) AND (0 > (SELECT t_and_chain_array_join.c0)) SETTINGS optimize_and_compare_chain = 1, optimize_redundant_comparisons = 0; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_and_chain_array_join;
