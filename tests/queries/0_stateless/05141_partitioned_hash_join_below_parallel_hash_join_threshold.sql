-- When the planner estimates the right table below `parallel_hash_join_threshold` rows,
-- `partitioned_hash` builds it on one thread. The pipeline then has the `hash` shape: one
-- `FillingRightJoinSide` transform and no squashing after the join. The hash table starts at the
-- estimated size and grows as `hash`'s does. A spill hands the kept build blocks to `GraceHashJoin`.
-- Results must match `hash` for every join kind.

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
SELECT 'inner', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 inner';

SELECT 'left', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 left';

SELECT 'right', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 right';

SELECT 'full', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 full';

SELECT '-- string key, nullable key';
SELECT 'string', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 string';

SELECT 'right nullable', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.kn SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 right nullable';

SELECT '-- any, semi, anti';
SELECT 'any', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p ANY LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p ANY LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 any';

SELECT 'semi', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sf_probe AS p SEMI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sf_probe AS p SEMI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 semi';

SELECT 'anti', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sf_probe AS p ANTI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_sf_probe AS p ANTI LEFT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 anti';

SELECT 'spill switch during the one-thread build', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 20000) AS pa)
SETTINGS log_comment = '05141 spill';

SELECT '-- right and full joins under a spill budget, with several probe streams and with one';
SELECT 'spill right', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 20000) AS pa)
SETTINGS log_comment = '05141 spill right';

SELECT 'spill right one stream', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 20000, max_threads = 1) AS pa)
SETTINGS log_comment = '05141 spill right one stream';

SELECT 'spill full one stream', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 20000, max_threads = 1) AS pa)
SETTINGS log_comment = '05141 spill full one stream';

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

SELECT 'asof', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT number % 500 AS k, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 2 AS ts, number AS p FROM numbers(20000)) AS p ASOF LEFT JOIN t_sf_asof AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT number % 500 AS k, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 2 AS ts, number AS p FROM numbers(20000)) AS p ASOF LEFT JOIN t_sf_asof AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 asof';

SELECT 'one row', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_one AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_one AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 one row';

SELECT 'one row right', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_one AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p RIGHT JOIN t_sf_one AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 one row right';

SELECT '50k', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 50k';

SELECT '50k full', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 50k full';

SELECT '-- a planner estimate far below the real row count only makes the table grow';
-- The hint reaches the join only through the greedy join-order optimizer without table statistics.
-- The hash-table statistics cache of the earlier identical query must not override it.
SET param__internal_join_table_stat_hints = '{"t_sf_50k": {"cardinality": 16, "distinct_keys": {"k": 16}}}';

SELECT 'low hint', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_50k AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS use_statistics = 0, query_plan_optimize_join_order_algorithm = 'greedy', collect_hash_table_stats_during_joins = 0, send_logs_level = 'error', log_comment = '05141 low hint';
SET param__internal_join_table_stat_hints = '{}';

SELECT '-- inner and spill again with the threshold off (parallel build, one partition per thread)';
SET parallel_hash_join_threshold = 0;

SELECT 'parallel inner', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p INNER JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '05141 parallel inner';

SELECT 'parallel spill', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_sf_probe AS p FULL JOIN t_sf_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 20000) AS pa)
SETTINGS log_comment = '05141 parallel spill';
SET parallel_hash_join_threshold = DEFAULT;

SYSTEM FLUSH LOGS query_log;

SELECT '-- one partition, rows inserted, table growth only for the low hint, the spill queries switched to grace';
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'],
    ProfileEvents['HashJoinInsertedRows'] > 0,
    ProfileEvents['HashJoinTableResizes'] > 0,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05141 %'
ORDER BY log_comment;

DROP TABLE t_sf_build;
DROP TABLE t_sf_probe;
DROP TABLE t_sf_asof;
DROP TABLE t_sf_one;
DROP TABLE t_sf_50k;
