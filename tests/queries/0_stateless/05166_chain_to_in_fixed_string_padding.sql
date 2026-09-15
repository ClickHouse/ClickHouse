-- Tags: no-old-analyzer
-- The rewrite being fixed here lives in the analyzer, and `EXPLAIN QUERY TREE` is analyzer-only.

-- https://github.com/ClickHouse/ClickHouse/issues/116852
-- The analyzer rewrites a comparison chain into `IN`/`NOT IN`, but `equals` compares the string
-- family zero-padded (`'a' = toFixedString('a', 2)` is true) while set membership is bytewise
-- (`'a' IN (toFixedString('a', 2))` is false). With a `String` column and `FixedString` constants the
-- rewritten chain contradicted its own operand: a conjunction was true while one of its terms was
-- false, and a disjunction was false while one of its terms was true.

DROP TABLE IF EXISTS t_chain_fixed_string;
CREATE TABLE t_chain_fixed_string (s String) ENGINE = Memory;
INSERT INTO t_chain_fixed_string VALUES ('a');

SELECT 'ground truth';
SELECT 'a' = toFixedString('a', 2), 'a' IN (toFixedString('a', 2));

SELECT 'not equals chain';
SELECT (s != toFixedString('a', 2) AND s != toFixedString('b', 2) AND s != toFixedString('c', 2)) AS chain,
       (s != toFixedString('a', 2)) AS single
FROM t_chain_fixed_string;
SELECT (s != toFixedString('a', 2) AND s != toFixedString('b', 2) AND s != toFixedString('c', 2)) AS chain,
       (s != toFixedString('a', 2)) AS single
FROM t_chain_fixed_string SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

SELECT 'equals chain';
SELECT (s = toFixedString('a', 2) OR s = toFixedString('b', 2) OR s = toFixedString('c', 2)) AS chain,
       (s = toFixedString('a', 2)) AS single
FROM t_chain_fixed_string;
SELECT (s = toFixedString('a', 2) OR s = toFixedString('b', 2) OR s = toFixedString('c', 2)) AS chain,
       (s = toFixedString('a', 2)) AS single
FROM t_chain_fixed_string SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'in where';
SELECT count() FROM t_chain_fixed_string WHERE s != toFixedString('a', 2) AND s != toFixedString('b', 2) AND s != toFixedString('c', 2);
SELECT count() FROM t_chain_fixed_string WHERE s = toFixedString('a', 2) OR s = toFixedString('b', 2) OR s = toFixedString('c', 2);

-- A chain of same-type constants is still rewritten.
SELECT 'still rewritten';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT s FROM t_chain_fixed_string WHERE s = 'a' OR s = 'b' OR s = 'c') WHERE explain LIKE '%function_name: in%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT s FROM t_chain_fixed_string WHERE s = toFixedString('a', 2) OR s = toFixedString('b', 2) OR s = toFixedString('c', 2)) WHERE explain LIKE '%function_name: in%';
SELECT count() FROM t_chain_fixed_string WHERE s = 'a' OR s = 'b' OR s = 'c';

-- The redundant-comparison pruning must not treat the converted constant as the whole truth
-- either: `s = toFixedString('a', 2)` converts to `s = 'a\0'`, which would wrongly imply `s != 'a'`.
SELECT 'redundant comparisons';
SELECT (s = toFixedString('a', 2) AND s != 'a') AS pruned,
       (s = toFixedString('a', 2)) AS equality,
       (s != 'a') AS inequality
FROM t_chain_fixed_string SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM t_chain_fixed_string WHERE s = toFixedString('a', 2) AND s != 'a' SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM t_chain_fixed_string WHERE s != toFixedString('a', 2) AND s = 'a' SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE t_chain_fixed_string;
