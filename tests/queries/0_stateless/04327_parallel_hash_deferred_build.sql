-- Regression test for the deferred exact-size parallel_hash build (no statistics hint): results
-- must be identical to join_algorithm = 'hash' for ALL/ANY joins on duplicate-heavy and unique
-- keys, including the non-joined output of RIGHT/FULL joins (the used-flags are keyed by the
-- stored block number). Each query is run with both algorithms and must print identical lines.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();

-- Mixed multiplicities on the build side: every 16th key has 17 rows (more than 8 duplicates =>
-- the chained BuildRefList path), every other 4th key has 3 rows, the rest are unique. All
-- non-key columns are functionally dependent on the key, so ANY joins are deterministic.
INSERT INTO t_build
SELECT k, concat('key_', toString(k)), k * 7
FROM
(
    SELECT number AS k, arrayJoin(range(multiIf(number % 16 = 0, 17, number % 4 = 0, 3, 1))) AS copy
    FROM numbers(100000)
);

-- Probe side: two rows per key, half of the keys miss the build side (=> non-joined rows for
-- LEFT, and build keys 0..49999 stay unmatched for RIGHT/FULL).
INSERT INTO t_probe
SELECT k, concat('key_', toString(k)), k * 13
FROM (SELECT 50000 + intDiv(number, 2) AS k FROM numbers(200000));

SET join_algorithm = 'hash';

SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;
SELECT 'any_left_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.k = r.k;
SELECT 'any_inner_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l ANY INNER JOIN t_build r ON l.k = r.k;
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s;
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s;
SELECT 'any_left_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.s = r.s;

SET join_algorithm = 'parallel_hash';
SET log_comment = '04327_parallel_hash_deferred_build';

SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;
SELECT 'any_left_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.k = r.k;
SELECT 'any_inner_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l ANY INNER JOIN t_build r ON l.k = r.k;
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s;
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s;
SELECT 'any_left_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.s = r.s;

-- Positive control: all parallel_hash queries above must have used the deferred exact-size
-- reserve (`HashJoinPreallocatedElementsInHashTables` is incremented only by the reserve; the
-- statistics-driven reserve is off because statistics collection is disabled).
SYSTEM FLUSH LOGS query_log;
SELECT
    'deferred build engaged',
    count() > 0,
    countIf(ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0) = count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04327_parallel_hash_deferred_build';

DROP TABLE t_build;
DROP TABLE t_probe;
