-- Tags: no-old-analyzer
-- The rewrite being fixed here lives in the analyzer, and `EXPLAIN QUERY TREE` is analyzer-only.

-- `equals` between two values of the same `Variant` type dispatches on the active alternatives, so a
-- `String` alternative compares equal to a `FixedString` alternative zero-padded, while set
-- membership is keyed on the discriminator and the value. A comparison chain over such a `Variant`
-- must not be rewritten into `IN`/`NOT IN`, the redundant-comparison pruning must not trust the
-- constant, and `arrayExists` must not become `has`.

SET enable_analyzer = 1;
SET allow_suspicious_variant_types = 1;
SET use_variant_default_implementation_for_comparisons = 1;
SET cast_string_to_variant_use_inference = 0;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;

DROP TABLE IF EXISTS t_chain_fixed_string_same_variant;
CREATE TABLE t_chain_fixed_string_same_variant (v Variant(String, FixedString(2))) ENGINE = Memory;
INSERT INTO t_chain_fixed_string_same_variant SELECT CAST('a'::String, 'Variant(String, FixedString(2))');

SELECT 'ground truth';
SELECT variantType(v),
       v = CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))'),
       has([v], CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))'))
FROM t_chain_fixed_string_same_variant;

SELECT 'equals chain';
SELECT v = CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))')
    OR v = CAST(toFixedString('b', 2), 'Variant(String, FixedString(2))')
    OR v = CAST(toFixedString('c', 2), 'Variant(String, FixedString(2))')
FROM t_chain_fixed_string_same_variant;

SELECT 'not equals chain';
SELECT v != CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))')
    AND v != CAST(toFixedString('b', 2), 'Variant(String, FixedString(2))')
    AND v != CAST(toFixedString('c', 2), 'Variant(String, FixedString(2))')
FROM t_chain_fixed_string_same_variant;

SELECT 'not rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT v FROM t_chain_fixed_string_same_variant
    WHERE v = CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))')
       OR v = CAST(toFixedString('b', 2), 'Variant(String, FixedString(2))')
       OR v = CAST(toFixedString('c', 2), 'Variant(String, FixedString(2))')) WHERE explain LIKE '%function_name: in%';

SELECT 'redundant comparisons';
SELECT count() FROM t_chain_fixed_string_same_variant
WHERE v = CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))') AND v != CAST('a'::String, 'Variant(String, FixedString(2))')
SETTINGS optimize_redundant_comparisons = 1;

SELECT 'arrayExists';
SELECT arrayExists(x -> x = CAST(toFixedString('a', 2), 'Variant(String, FixedString(2))'), [v])
FROM t_chain_fixed_string_same_variant
SETTINGS optimize_rewrite_array_exists_to_has = 1;

DROP TABLE t_chain_fixed_string_same_variant;
