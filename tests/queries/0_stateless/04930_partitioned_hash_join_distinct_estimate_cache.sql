-- Warm-run per-partition distinct-key cache for `partitioned_hash`: a second run of the same
-- query structure reuses the previous run's cached distinct-key counts
-- (`PartitionedHashJoinDistinctEstimateReused`), skipping the per-row HyperLogLog sketch feed of
-- the build fill, and produces identical results.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
-- The ProfileEvents assertions below read this server's query_log; with parallel replicas the
-- join builds (and their events) can land on other replicas. Runtime filters are unrelated to
-- this mechanism and pinned off to keep the plan identical across runs.
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 1;

DROP TABLE IF EXISTS t_dec_build;
DROP TABLE IF EXISTS t_dec_probe;

CREATE TABLE t_dec_build (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dec_probe (k UInt64, p UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_dec_build SELECT number, concat('v', toString(number)) FROM numbers(300000);
INSERT INTO t_dec_probe SELECT number % 300000, number FROM numbers(600000);

SELECT '-- cold run (no cache entry yet)';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_dec_probe AS p INNER JOIN t_dec_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'dec_cold';

SELECT '-- warm run (same query structure: reuses the cached per-partition distinct counts)';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_dec_probe AS p INNER JOIN t_dec_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'dec_warm';

SYSTEM FLUSH LOGS query_log;

SELECT '-- cold run: never reuses, but the partitioned path did engage';
SELECT
    ProfileEvents['PartitionedHashJoinDistinctEstimateReused'],
    ProfileEvents['PartitionedHashJoinPartitions'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'dec_cold' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- warm run: reuses the cached distinct-key counts';
SELECT
    ProfileEvents['PartitionedHashJoinDistinctEstimateReused'] > 0,
    ProfileEvents['PartitionedHashJoinPartitions'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'dec_warm' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_dec_build;
DROP TABLE t_dec_probe;
