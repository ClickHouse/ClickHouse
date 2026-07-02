-- Test: a WHERE literal that does NOT fit a LowCardinality(FixedString) key column
-- makes the accurate-or-null probe yield NULL, so the key transform must bail out
-- gracefully (no pruning condition, correct results) instead of throwing.
DROP TABLE IF EXISTS t_lc_fs_overlong;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_fs_overlong;
CREATE TABLE t_lc_fs_overlong (k LowCardinality(FixedString(8))) ENGINE = MergeTree ORDER BY k PARTITION BY sipHash64(k) % 8;
INSERT INTO t_lc_fs_overlong SELECT toFixedString(leftPad(toString(number), 8, '0'), 8) FROM numbers(200);

SELECT 'overlong condition not built',
       countIf(explain ILIKE '%moduloLegacy(sipHash64(k), 8)%') = 0 AS bailed
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc_fs_overlong WHERE k = '0000000123456');

SELECT 'overlong result', count() FROM t_lc_fs_overlong WHERE k = '0000000123456';

DROP TABLE t_lc_fs_overlong;
