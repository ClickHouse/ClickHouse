-- Two `Nullable` keys: the deferred build counts the keys with the null map that combines both, as the insert does.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k1 Nullable(UInt64), k2 Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k1 Nullable(UInt64), k2 Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build
SELECT
    if(number % 7 = 0, NULL, number % 5000) AS k1,
    if(number % 11 = 0, NULL, number % 4000) AS k2,
    number AS v
FROM numbers(200000);

INSERT INTO t_probe
SELECT
    if(number % 13 = 0, NULL, number % 5000) AS k1,
    if(number % 17 = 0, NULL, number % 4000) AS k2,
    number AS v
FROM numbers(50000);

SET enable_hash_join_deferred_build = 0;
SELECT 'multi nullable', count(), sum(cityHash64(l.v, r.v))
FROM t_probe l INNER JOIN t_build r ON l.k1 = r.k1 AND l.k2 = r.k2;

SET enable_hash_join_deferred_build = 1;
SELECT 'multi nullable', count(), sum(cityHash64(l.v, r.v))
FROM t_probe l INNER JOIN t_build r ON l.k1 = r.k1 AND l.k2 = r.k2;

DROP TABLE t_build;
DROP TABLE t_probe;
