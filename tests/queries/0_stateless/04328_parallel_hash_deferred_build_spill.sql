-- Regression test for the handoff of a deferred (exact-size) parallel_hash build to
-- GraceHashJoin: with a small max_bytes_before_external_join the spill threshold is crossed in
-- the middle of the build phase, while the right blocks are still only buffered (no in-memory
-- map exists yet), so `SpillingHashJoin` must hand the buffered blocks over to `GraceHashJoin`
-- without losing or duplicating rows. The first batch (plain hash join, no spilling) is the
-- reference; the second batch must print identical lines.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET grace_hash_join_initial_buckets = 4;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY ();

-- Mixed multiplicities (every 16th key has 17 rows, every other 4th key has 3 rows, the rest
-- unique), non-key columns functionally dependent on the key (ANY joins stay deterministic).
-- About 50 MiB of buffered columns: comfortably above the spill threshold used below.
INSERT INTO t_build
SELECT k, concat('key_', toString(k)), k * 7
FROM
(
    SELECT number AS k, arrayJoin(range(multiIf(number % 16 = 0, 17, number % 4 = 0, 3, 1))) AS copy
    FROM numbers(500000)
);

INSERT INTO t_probe
SELECT k, concat('key_', toString(k)), k * 13
FROM (SELECT 250000 + intDiv(number, 2) AS k FROM numbers(1000000));

SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;

SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;
SELECT 'any_left_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.k = r.k;
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s;
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s;

SET join_algorithm = 'parallel_hash';
SET max_bytes_before_external_join = 16777216; -- 16 MiB: crossed while blocks are still buffered
SET log_comment = '04328_parallel_hash_deferred_build_spill';

SELECT 'inner_all_u64', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full_all_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;
SELECT 'any_left_u64', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_probe l ANY LEFT JOIN t_build r ON l.k = r.k;
SELECT 'inner_all_str', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.s = r.s;
SELECT 'full_all_str', count(), sum(cityHash64(l.s, l.v, r.s, r.v)) FROM t_probe l FULL JOIN t_build r ON l.s = r.s;

-- Positive control: every parallel_hash query above must actually have switched to GraceHashJoin.
SYSTEM FLUSH LOGS query_log;
SELECT
    'switched to grace join',
    count() > 0,
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) = count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04328_parallel_hash_deferred_build_spill';

DROP TABLE t_build;
DROP TABLE t_probe;
