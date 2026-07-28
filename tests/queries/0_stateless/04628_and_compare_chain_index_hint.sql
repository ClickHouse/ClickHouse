-- Comparisons derived by `optimize_and_compare_chain` are wrapped in `indexHint`:
-- they still prune the read set but cost nothing per row.

SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS t_chain_hint;
CREATE TABLE t_chain_hint (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024;
INSERT INTO t_chain_hint SELECT number, number + 1 FROM numbers(65536);

-- The derived comparison appears wrapped in `indexHint` in the analyzed tree.
SELECT 'derived_as_hint';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_hint WHERE a < b AND b < 1000)
    WHERE explain LIKE '%function_name: indexHint%';

-- Primary key pruning still works through the hint: only the granules of `a < 1000` are read.
SELECT 'pk_pruning';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_chain_hint WHERE a < b AND b < 1000)
    WHERE explain LIKE '%Condition: (a in (-Inf, 999])%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_chain_hint WHERE a < b AND b < 1000)
    WHERE explain LIKE '%Granules: 1/64%';

-- The derived comparison (an expensive expression here) does not leak into the PREWHERE filter.
SELECT 'no_prewhere_leak';
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_chain_hint WHERE hex(sipHash64(a)) < hex(b) AND hex(b) < 'Z')
    WHERE explain LIKE '%Prewhere filter column%' AND explain LIKE '%sipHash64%';

-- Results are identical with the optimization enabled and disabled.
SELECT 'results';
SELECT count() FROM t_chain_hint WHERE a < b AND b < 1000 SETTINGS optimize_and_compare_chain = 1;
SELECT count() FROM t_chain_hint WHERE a < b AND b < 1000 SETTINGS optimize_and_compare_chain = 0;

-- A contradicting derived comparison is added plain, so the AND still folds to `false`.
SELECT 'contradiction';
SELECT count() FROM t_chain_hint WHERE a < b AND b < 5 AND a > 10;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_hint WHERE a < b AND b < 5 AND a > 10)
    WHERE explain LIKE '%function_name: indexHint%';

-- A derived equality contradicting a `!=` conjunct also stays plain and folds the AND.
SELECT 'not_equals_contradiction';
SELECT count() FROM t_chain_hint WHERE a = b AND b = 5 AND a != 5;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_hint WHERE a = b AND b = 5 AND a != 5)
    WHERE explain LIKE '%function_name: indexHint%';

DROP TABLE t_chain_hint;
