-- Regression test for a logical error in `tryOptimizeAndCompareNotEqualsChain`.
--
-- The AND compare/notEquals-chain pruning classified each operand's constant side and asserted the
-- constant is non-NULL. That assumption is wrong: a comparison operand can carry a NULL-valued
-- constant whose type is non-Nullable (e.g. a NULL-valued `Variant`). The pass only early-returns
-- when the whole AND is Nullable, but with `use_variant_default_implementation_for_comparisons = 0`
-- a comparison against a NULL-valued `Variant` constant is non-nullable, so the AND is non-nullable
-- and the NULL constant reached the assertion, aborting the server in debug/sanitizer builds.
-- (STID 2508-5318, found by the AST fuzzer.)
--
-- Analysis (the `EXPLAIN QUERY TREE` below runs the logical-expression optimizer, where the abort
-- happened) must complete without a logical error. The NULL-valued constant cannot be used as a
-- bound, so the operand is kept as-is; the `and` node with both comparisons therefore survives.

SET enable_analyzer = 1;
SET use_variant_default_implementation_for_comparisons = 0;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_04613;
CREATE TABLE t_04613 (a String, b String) ENGINE = MergeTree ORDER BY tuple();

-- The exact fuzzed shape: comparison against a NULL-valued Variant constant on the right (lessOrEquals).
SELECT 'lessOrEquals_rhs_null_variant';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (a >= 'm') AND (b <= _CAST('ᴺᵁᴸᴸ', 'Variant(Enum8(\'v0\' = 0, \'v1\' = 1), LineString)'))
) WHERE explain ILIKE '%function_name: lessOrEquals%';

-- Constant on the left side must be handled the same way (greaterOrEquals).
SELECT 'greaterOrEquals_lhs_null_variant';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (_CAST('ᴺᵁᴸᴸ', 'Variant(Enum8(\'v0\' = 0), String)') >= b) AND (a >= 'm')
) WHERE explain ILIKE '%function_name: greaterOrEquals%';

-- notEquals is what feeds the NOT IN conversion; a NULL-valued constant must not reach it.
SELECT 'notEquals_null_variant';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (b != _CAST('ᴺᵁᴸᴸ', 'Variant(Int8, String)')) AND (b != 'p') AND (b != 'q') AND (b != 'r')
) WHERE explain ILIKE '%function_name: notEquals%';

-- equals against a NULL-valued constant inside an AND chain.
SELECT 'equals_null_variant';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (b = _CAST('ᴺᵁᴸᴸ', 'Variant(Int8, String)')) AND (a >= 'm')
) WHERE explain ILIKE '%function_name: equals%';

-- Strict less/greater operators in the chain.
SELECT 'less_greater_null_variant';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (b < _CAST('ᴺᵁᴸᴸ', 'Variant(Int8, String)')) AND (a > 'k') AND (b > 'a')
) WHERE explain ILIKE '%function_name: less%';

-- The transitive AND-compare-chain pass runs first on the same node; exercise it too.
SELECT 'and_compare_chain_null_variant';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (a < b) AND (b < _CAST('ᴺᵁᴸᴸ', 'Variant(Enum8(\'v0\' = 0), LineString)')) AND (a > 'k')
    SETTINGS optimize_and_compare_chain = 1
) WHERE explain ILIKE '%function_name: and%';

-- A NULL-valued constant mixed with a real notEquals chain long enough to trigger NOT IN conversion:
-- the real constants still convert to NOT IN; only the NULL operand is kept as-is.
SELECT 'mixed_null_and_real_notequals';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT * FROM t_04613 WHERE (b != _CAST('ᴺᵁᴸᴸ', 'Variant(Int8, String)')) AND (b != 'p') AND (b != 'q') AND (b != 'r') AND (a >= 'm')
) WHERE explain ILIKE '%notIn%' OR explain ILIKE '%function_name: notEquals%';

DROP TABLE t_04613;
