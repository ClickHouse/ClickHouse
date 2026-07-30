-- Part 1 regression: `SpillingHashJoin::addBlockToJoin` uses the live byte count for
-- `ConcurrentHashJoin` (buffered blocks only during a deferred build), while the projection-based
-- spill decision runs once at `onBuildPhaseFinish`. The buffered payload here is well under half of
-- `max_bytes_before_external_join`, but the projected hash-table buffers for the buffered row count
-- would have tripped the old per-block projection check; the build must still switch to
-- GraceHashJoin at finish and return correct results.

SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET grace_hash_join_initial_buckets = 4;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();

-- 120k source rows but only 12k distinct keys: modest buffered bytes, large projected map size.
INSERT INTO t_build
SELECT intDiv(number, 10) AS k, number * 3 AS v
FROM numbers(120000);

INSERT INTO t_probe
SELECT intDiv(number, 2) AS k, number * 5 AS v
FROM numbers(40000);

SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;

SELECT 'hash_ref', count(), sum(cityHash64(l.k, l.v, r.v))
FROM t_probe l INNER JOIN t_build r ON l.k = r.k;

SET join_algorithm = 'parallel_hash';
-- Buffered columns are ~2 MiB; projected map for 120k rows is much larger. Half-threshold is 4 MiB,
-- so the per-block live-byte check must not switch early; the terminal projection check must.
SET max_bytes_before_external_join = 8388608; -- 8 MiB
SET log_comment = '04404_spilling_hash_join_two_step_spill_decision';

SELECT 'parallel_hash', count(), sum(cityHash64(l.k, l.v, r.v))
FROM t_probe l INNER JOIN t_build r ON l.k = r.k;

SYSTEM FLUSH LOGS query_log;
SELECT
    'switched at finish not per-block',
    count() = 1,
    countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04404_spilling_hash_join_two_step_spill_decision';

DROP TABLE t_build;
DROP TABLE t_probe;
