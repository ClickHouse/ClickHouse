-- Tags: no-old-analyzer
-- The rewrite being fixed here lives in the analyzer, and `EXPLAIN QUERY TREE` is analyzer-only.

-- `equals` between a `String` and a `Variant` holding a `FixedString` compares the strings
-- zero-padded, like a plain `FixedString` constant, while set membership is bytewise. A comparison
-- chain over such constants must not be rewritten into `IN`/`NOT IN`, and the redundant-comparison
-- pruning must not trust the converted constant either.

SET enable_analyzer = 1;
SET allow_suspicious_variant_types = 1;
SET use_variant_default_implementation_for_comparisons = 1;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;

DROP TABLE IF EXISTS t_chain_fixed_string_variant;
CREATE TABLE t_chain_fixed_string_variant (s String) ENGINE = Memory;
INSERT INTO t_chain_fixed_string_variant VALUES ('a');

SELECT 'ground truth';
SELECT s = CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)') FROM t_chain_fixed_string_variant;

SELECT 'equals chain';
SELECT (s = CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)')
        OR s = CAST(toFixedString('b', 2), 'Variant(FixedString(2), UInt64)')
        OR s = CAST(toFixedString('c', 2), 'Variant(FixedString(2), UInt64)')) AS chain,
       (s = CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)')) AS single
FROM t_chain_fixed_string_variant;

SELECT 'not equals chain';
SELECT (s != CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)')
        AND s != CAST(toFixedString('b', 2), 'Variant(FixedString(2), UInt64)')
        AND s != CAST(toFixedString('c', 2), 'Variant(FixedString(2), UInt64)')) AS chain,
       (s != CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)')) AS single
FROM t_chain_fixed_string_variant;

SELECT 'not rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT s FROM t_chain_fixed_string_variant
    WHERE s = CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)')
       OR s = CAST(toFixedString('b', 2), 'Variant(FixedString(2), UInt64)')
       OR s = CAST(toFixedString('c', 2), 'Variant(FixedString(2), UInt64)')) WHERE explain LIKE '%function_name: in%';

SELECT 'redundant comparisons';
SELECT count() FROM t_chain_fixed_string_variant
WHERE s = CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)') AND s != 'a'
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM t_chain_fixed_string_variant
WHERE s = CAST(toFixedString('a', 2), 'Variant(FixedString(2), UInt64)') AND s = 'a'
SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE t_chain_fixed_string_variant;
