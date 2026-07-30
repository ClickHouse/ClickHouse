-- Regression test for the deferred (exact-size) parallel_hash build -> GraceHashJoin spill handoff
-- with a LowCardinality(String) join key. The deferred build's spill projection sizes the arena key
-- bytes from the buffered key column; `LowCardinality(String)` keeps the dictionary tiny while the
-- replay expands the key per row, so this exercises a duplicate-heavy "hot" long low-cardinality
-- value (2000 build rows share one long string). The plain hash batch is the reference; the
-- parallel_hash batch must print identical lines and must actually have spilled to GraceHashJoin.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of the (small) build size
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET grace_hash_join_initial_buckets = 4;
-- Keep the whole build side on one node so the per-replica buffer deterministically crosses the
-- small spill threshold (see 04328_parallel_hash_deferred_build_spill).
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe (k LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY ();

-- Build: 2000 rows share one long low-cardinality "hot" key (the LC dictionary stays a single long
-- string while the replay would copy it per row), plus ~6000 rows spread over k_0..k_1499. The hot
-- key is absent from the probe, so it adds many buffered build rows without a cartesian blow-up.
INSERT INTO t_build
SELECT if(number < 2000, repeat('hot_', 50), concat('k_', toString(number % 1500))) AS k, number AS v
FROM numbers(8000);

INSERT INTO t_probe
SELECT concat('k_', toString(number % 1500)) AS k, number AS v
FROM numbers(8000);

SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

SET join_algorithm = 'parallel_hash';
SET max_bytes_before_external_join = 131072; -- 128 KiB: crossed early while blocks are still buffered
SET log_comment = '04403_parallel_hash_deferred_build_spill_lowcardinality';

SELECT 'inner', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l INNER JOIN t_build r ON l.k = r.k;
SELECT 'left', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l LEFT JOIN t_build r ON l.k = r.k;
SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l RIGHT JOIN t_build r ON l.k = r.k;
SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_probe l FULL JOIN t_build r ON l.k = r.k;

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
    AND log_comment = '04403_parallel_hash_deferred_build_spill_lowcardinality';

DROP TABLE t_build;
DROP TABLE t_probe;
