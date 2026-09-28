-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/108776
-- Partition pruning was silently disabled when a LowCardinality(FixedString) key column
-- was wrapped in a function in the partition key (e.g. PARTITION BY sipHash64(k) % N) and the
-- WHERE compared it against a String literal. Transforming the String constant into key space
-- failed because the LowCardinality wrapper is not nullable-able, so the accuracy probe in
-- applyDeterministicDagToColumn rejected the String -> LowCardinality(FixedString) cast and the
-- partition condition degraded to "true" (scan all partitions). The test asserts the
-- optimization is PRESERVED: a real partition condition is built and prunes. Correct-result
-- checks alone cannot catch this regression.

SET allow_suspicious_low_cardinality_types = 1;

-- The fix must work for LowCardinality(FixedString) and LowCardinality(Nullable(FixedString)),
-- and must NOT regress the shapes that already worked (plain FixedString, LowCardinality(String)).
-- The String literal in WHERE is the trigger: with toFixedString(...) the constant is already
-- FixedString and the broken cast path is never reached.

DROP TABLE IF EXISTS t_lc_fs;
CREATE TABLE t_lc_fs (k LowCardinality(FixedString(8))) ENGINE = MergeTree ORDER BY k PARTITION BY sipHash64(k) % 8;
INSERT INTO t_lc_fs SELECT toFixedString(leftPad(toString(number), 8, '0'), 8) FROM numbers(200);
SELECT 'LowCardinality(FixedString) condition built',
       countIf(explain ILIKE '%moduloLegacy(sipHash64(k), 8)%') > 0 AS condition_built
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc_fs WHERE k = '00000001');

DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (k FixedString(8)) ENGINE = MergeTree ORDER BY k PARTITION BY sipHash64(k) % 8;
INSERT INTO t_fs SELECT toFixedString(leftPad(toString(number), 8, '0'), 8) FROM numbers(200);
SELECT 'FixedString condition built',
       countIf(explain ILIKE '%moduloLegacy(sipHash64(k), 8)%') > 0 AS condition_built
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fs WHERE k = '00000001');

DROP TABLE IF EXISTS t_lc_s;
CREATE TABLE t_lc_s (k LowCardinality(String)) ENGINE = MergeTree ORDER BY k PARTITION BY sipHash64(k) % 8;
INSERT INTO t_lc_s SELECT leftPad(toString(number), 8, '0') FROM numbers(200);
SELECT 'LowCardinality(String) condition built',
       countIf(explain ILIKE '%moduloLegacy(sipHash64(k), 8)%') > 0 AS condition_built
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc_s WHERE k = '00000001');

DROP TABLE IF EXISTS t_lc_n_fs;
CREATE TABLE t_lc_n_fs (k LowCardinality(Nullable(FixedString(8)))) ENGINE = MergeTree ORDER BY k PARTITION BY sipHash64(k) % 8 SETTINGS allow_nullable_key = 1;
INSERT INTO t_lc_n_fs SELECT toFixedString(leftPad(toString(number), 8, '0'), 8) FROM numbers(200);
SELECT 'LowCardinality(Nullable(FixedString)) condition built',
       countIf(explain ILIKE '%moduloLegacy(sipHash64(k), 8)%') > 0 AS condition_built
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc_n_fs WHERE k = '00000001');

-- Results stay correct after pruning.
SELECT 'correctness match', count() FROM t_lc_fs WHERE k = '00000001';
SELECT 'correctness no-match', count() FROM t_lc_fs WHERE k = '99999999';

DROP TABLE t_lc_fs;
DROP TABLE t_fs;
DROP TABLE t_lc_s;
DROP TABLE t_lc_n_fs;
