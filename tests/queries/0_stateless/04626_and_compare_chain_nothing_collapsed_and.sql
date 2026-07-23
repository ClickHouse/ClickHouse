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
--    AND is still folded. In `x > 3 AND x > 5`, `x > 3` is redundant and pruned, so the enabled tree
--    has strictly fewer `greater` nodes than the disabled tree.
SELECT
    (SELECT count() FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_redundant_comparisons = 1) WHERE explain LIKE '%function_name: greater,%')
  < (SELECT count() FROM (EXPLAIN QUERY TREE SELECT tuple((materialize(toNullable(NULL)) = 1) AND (materialize(toInt32(5)) > 3) AND (materialize(toInt32(5)) > 5) AND (assumeNotNull(materialize(toNullable(NULL))) = 2)) SETTINGS optimize_redundant_comparisons = 0) WHERE explain LIKE '%function_name: greater,%');
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
