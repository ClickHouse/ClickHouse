-- A right table the planner estimates below `parallel_hash_join_threshold` is built by
-- `partitioned_hash` on one fill thread: the pipeline keeps the `hash` shape (one filling transform, no
-- block merger after the join), the table grows from the estimate as `hash`'s does, and the spill switch
-- hands the stored blocks to grace. Results must match `hash` for every join kind.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;
SET max_threads = 4;
SET join_algorithm = 'partitioned_hash';

DROP TABLE IF EXISTS t_sf_build;
DROP TABLE IF EXISTS t_sf_probe;
DROP TABLE IF EXISTS t_sf_asof;
DROP TABLE IF EXISTS t_sf_one;
DROP TABLE IF EXISTS t_sf_50k;

CREATE TABLE t_sf_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT number AS k, toString(number) AS ks, if(number % 9 = 0, NULL, toUInt64(number)) AS kn, number + 1000000 AS v
FROM numbers(3000);

CREATE TABLE t_sf_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT number % 4000 AS k, toString(number % 4000) AS ks, number AS p
FROM numbers(50000);

SELECT '-- one filling transform below the threshold, max_threads of them above it';
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (EXPLAIN PIPELINE SELECT count() FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k);
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (EXPLAIN PIPELINE SELECT count() FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS parallel_hash_join_threshold = 0);

SELECT '-- inner / left / right / full all, UInt64 key';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 inner';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 left';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 right';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 full';

SELECT '-- string key, nullable key';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.ks = b.ks SETTINGS log_comment = '05141 string';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.kn SETTINGS log_comment = '05141 right nullable';

SELECT '-- any, semi, anti';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p ANY LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p ANY LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 any';
SELECT count(), sum(cityHash64(p.p)) FROM t_sf_probe AS p SEMI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p)) FROM t_sf_probe AS p SEMI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 semi';
SELECT count(), sum(cityHash64(p.p)) FROM t_sf_probe AS p ANTI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p)) FROM t_sf_probe AS p ANTI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 anti';

SELECT '-- spill switch during the single-thread fill';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k
SETTINGS max_bytes_before_external_join = 20000, log_comment = '05141 spill';

SELECT '-- right join, and a single-stream left, under a spill threshold';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k
SETTINGS max_bytes_before_external_join = 20000, log_comment = '05141 spill right';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k
SETTINGS max_bytes_before_external_join = 20000, max_threads = 1, log_comment = '05141 spill right one stream';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k
SETTINGS max_bytes_before_external_join = 20000, max_threads = 1, log_comment = '05141 spill full one stream';

SELECT '-- the spilling wrapper keeps the shape: one filling transform below the threshold, delayed ports';
SELECT countIf(explain LIKE '%FillingRightJoinSide%'), countIf(explain LIKE '%DelayedPorts%') > 0
FROM (EXPLAIN PIPELINE SELECT count() FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS max_bytes_before_external_join = 1000000000);
SELECT countIf(explain LIKE '%FillingRightJoinSide%'), countIf(explain LIKE '%DelayedPorts%') > 0
FROM (EXPLAIN PIPELINE SELECT count() FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS max_bytes_before_external_join = 1000000000, parallel_hash_join_threshold = 0);

SELECT '-- asof, a one-row build and a 50000-row build, all below the threshold';
CREATE TABLE t_sf_asof ENGINE = MergeTree ORDER BY tuple() AS
SELECT number % 500 AS k, toDateTime('2020-01-01 00:00:00', 'UTC') + number AS ts, number AS v FROM numbers(3000);
CREATE TABLE t_sf_one ENGINE = MergeTree ORDER BY tuple() AS SELECT toUInt64(7) AS k, toUInt64(42) AS v;
CREATE TABLE t_sf_50k ENGINE = MergeTree ORDER BY tuple() AS SELECT number AS k, number * 2 AS v FROM numbers(50000);
SELECT count(), sum(cityHash64(p.p, b.v))
FROM (SELECT number % 500 AS k, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 2 AS ts, number AS p FROM numbers(20000)) AS p
ASOF LEFT JOIN t_sf_asof AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v))
FROM (SELECT number % 500 AS k, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 2 AS ts, number AS p FROM numbers(20000)) AS p
ASOF LEFT JOIN t_sf_asof AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS log_comment = '05141 asof';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_one AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_one AS b ON p.k = b.k SETTINGS log_comment = '05141 one row';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_one AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_one AS b ON p.k = b.k SETTINGS log_comment = '05141 one row right';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k SETTINGS log_comment = '05141 50k';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_50k AS b ON p.k = b.k SETTINGS log_comment = '05141 50k full';

SELECT '-- a planner estimate far below the truth costs table doublings, nothing else';
-- The hint reaches the join only through the greedy join-order optimizer without table statistics, and the
-- hash-table statistics cache of the earlier identical query must not override it.
SET param__internal_join_table_stat_hints = '{"t_sf_50k": {"cardinality": 16, "distinct_keys": {"k": 16}}}';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k
SETTINGS use_statistics = 0, query_plan_optimize_join_order_algorithm = 'greedy', collect_hash_table_stats_during_joins = 0, send_logs_level = 'error', log_comment = '05141 low hint';
SET param__internal_join_table_stat_hints = '{}';

SELECT '-- the same shapes on today''s parallel fill';
SET parallel_hash_join_threshold = 0;
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 inner';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 left';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 right';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 full';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.ks = b.ks SETTINGS log_comment = '05141 t0 string';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.kn SETTINGS log_comment = '05141 t0 right nullable';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p ANY LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 any';
SELECT count(), sum(cityHash64(p.p)) FROM t_sf_probe AS p SEMI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 semi';
SELECT count(), sum(cityHash64(p.p)) FROM t_sf_probe AS p ANTI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS log_comment = '05141 t0 anti';
SELECT count(), sum(cityHash64(p.p, b.v)) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k
SETTINGS max_bytes_before_external_join = 20000, log_comment = '05141 t0 spill';
SET parallel_hash_join_threshold = DEFAULT;

SYSTEM FLUSH LOGS query_log;

SELECT '-- one partition, rows inserted, table doublings only for the low hint, the spill queries switched to grace';
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'],
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0,
    ProfileEvents['PartitionedHashJoinTableResizes'] > 0,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05141 %'
ORDER BY log_comment;

DROP TABLE t_sf_build;
DROP TABLE t_sf_probe;
DROP TABLE t_sf_asof;
DROP TABLE t_sf_one;
DROP TABLE t_sf_50k;
