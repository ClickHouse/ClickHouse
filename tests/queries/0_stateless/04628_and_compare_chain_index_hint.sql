-- Comparisons derived by `optimize_and_compare_chain` are wrapped in `indexHint`:
-- they still prune the read set but cost nothing per row.

SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;
-- Every assertion below needs the derivation to actually run, so the work budget cannot be
-- left to the test runner's randomization (a low budget makes the pass derive nothing).
SET optimize_and_compare_chain_max_hash_work = 5000000;

DROP TABLE IF EXISTS t_chain_hint;
CREATE TABLE t_chain_hint (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
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

-- A condition derived across a join stays executable: no hint, one extra `less`.
SELECT 'join_derived_stays_plain';
DROP TABLE IF EXISTS t_chain_hint_r;
CREATE TABLE t_chain_hint_r (c UInt64, d UInt64) ENGINE = MergeTree ORDER BY c
    SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_chain_hint_r SELECT number, number + 2 FROM numbers(65536);
SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM t_chain_hint AS l JOIN t_chain_hint_r AS r ON l.a = r.c WHERE l.b < r.d AND r.d < 100)
    WHERE explain LIKE '%function_name: indexHint%';
SELECT
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT count() FROM t_chain_hint AS l JOIN t_chain_hint_r AS r ON l.a = r.c WHERE l.b < r.d AND r.d < 100
        SETTINGS optimize_and_compare_chain = 1) WHERE explain LIKE '%function_name: less,%')
    -
    (SELECT count() FROM (EXPLAIN QUERY TREE
        SELECT count() FROM t_chain_hint AS l JOIN t_chain_hint_r AS r ON l.a = r.c WHERE l.b < r.d AND r.d < 100
        SETTINGS optimize_and_compare_chain = 0) WHERE explain LIKE '%function_name: less,%');
SELECT count() FROM t_chain_hint AS l JOIN t_chain_hint_r AS r ON l.a = r.c WHERE l.b < r.d AND r.d < 100
    SETTINGS optimize_and_compare_chain = 1;
SELECT count() FROM t_chain_hint AS l JOIN t_chain_hint_r AS r ON l.a = r.c WHERE l.b < r.d AND r.d < 100
    SETTINGS optimize_and_compare_chain = 0;
-- A user-written hint must not suppress executable derived conjuncts.
SELECT 'hint_does_not_suppress';
SELECT count() FROM t_chain_hint WHERE a = b AND b = 5 AND indexHint(a = 5) AND a != 5;
SELECT count() FROM (EXPLAIN QUERY TREE
    SELECT count() FROM t_chain_hint AS l JOIN t_chain_hint_r AS r ON l.a = r.c
    WHERE l.b < r.d AND r.d < 100 AND indexHint(l.b < 100))
    WHERE explain LIKE '%function_name: less,%';

DROP TABLE t_chain_hint_r;

-- Same-named columns of different tables never join one chain: the column source is part
-- of query tree node identity, so no condition is derived and no false conflict is found.
SELECT 'same_name_isolation';
DROP TABLE IF EXISTS t_chain_hint_n1;
DROP TABLE IF EXISTS t_chain_hint_n2;
CREATE TABLE t_chain_hint_n1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a AS SELECT 20, 20;
CREATE TABLE t_chain_hint_n2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a AS SELECT 7, 1;
SELECT count() FROM t_chain_hint_n1, t_chain_hint_n2
    WHERE t_chain_hint_n1.a <= t_chain_hint_n1.b AND t_chain_hint_n2.b < 5;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_hint_n1, t_chain_hint_n2
    WHERE t_chain_hint_n1.a <= t_chain_hint_n1.b AND t_chain_hint_n2.b < 5)
    WHERE explain LIKE '%function_name: indexHint%' OR explain LIKE '%function_name: lessOrEquals,%';
SELECT count() FROM t_chain_hint_n1, t_chain_hint_n2
    WHERE t_chain_hint_n1.a = t_chain_hint_n1.b AND t_chain_hint_n1.b = 20 AND t_chain_hint_n2.a != 20;
DROP TABLE t_chain_hint_n1;
DROP TABLE t_chain_hint_n2;

DROP TABLE t_chain_hint;
