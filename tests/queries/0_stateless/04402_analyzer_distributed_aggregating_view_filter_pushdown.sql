-- Tags: shard, no-parallel-replicas, no-random-merge-tree-settings
-- Regression test for issue #108284: a predicate on a GROUP BY key that reaches a shard as `HAVING`
-- must be applied before aggregation on the shard, because the shard processes the query only up to
-- `WithMergeableState` and does not execute `HAVING` itself.

set enable_analyzer = 1;
-- Force both shards through the remote path so the plan we inspect is the one built by the shard,
-- not the initiator-planned local replica.
set prefer_localhost_replica = 0;
-- With a serialized query plan the shard receives a plan (not SQL) and EXPLAIN distributed=1 cannot
-- introspect its index usage; pin it off so the shard-side pruning is visible.
set serialize_query_plan = 0;
-- optimize_skip_unused_shards adds a condition on the sharding key to the shard query by itself.
set optimize_skip_unused_shards = 0;

DROP TABLE IF EXISTS t_local_04402;
DROP TABLE IF EXISTS t_dist_04402;
DROP VIEW IF EXISTS v_agg_04402;
DROP VIEW IF EXISTS v_hav_04402;
DROP TABLE IF EXISTS t_dates_04402;

CREATE TABLE t_local_04402 (date Date, login UInt64, x Float64)
    ENGINE = MergeTree PARTITION BY date ORDER BY (date, login);

CREATE TABLE t_dist_04402 AS t_local_04402
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_local_04402', sipHash64(login));

-- Aggregating view over the Distributed table; 'date' is a GROUP BY key (and partition key).
CREATE VIEW v_agg_04402 AS SELECT date, login, sum(x) AS s FROM t_dist_04402 GROUP BY date, login;

-- The same, but the grouping-key predicate is in the view's own HAVING.
CREATE VIEW v_hav_04402 AS
    SELECT date, login, sum(x) AS s FROM t_dist_04402 GROUP BY date, login HAVING date = '2024-01-05';

CREATE TABLE t_dates_04402 (date Date) ENGINE = MergeTree ORDER BY date;
INSERT INTO t_dates_04402 VALUES ('2024-01-05');

INSERT INTO t_local_04402
    SELECT toDate('2024-01-01') + (number % 10), number % 1000, number % 7
    FROM numbers(100000);

SELECT '--- outer filter on aggregating view';
SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date;
SELECT
    countIf(explain ILIKE '%Condition: (date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
);

SELECT '--- HAVING inside aggregating view';
SELECT date, sum(s) FROM v_hav_04402 GROUP BY date;
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_hav_04402 GROUP BY date
);

SELECT '--- HAVING on Distributed table';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date = '2024-01-05';
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date = '2024-01-05'
);

-- The aggregate conjunct stays in HAVING, the grouping-key conjunct is still used for pruning.
SELECT '--- HAVING with grouping key and aggregate conjuncts';
SELECT date, login, sum(x) AS s FROM t_dist_04402 GROUP BY date, login HAVING date = '2024-01-05' AND s > 63 ORDER BY login LIMIT 3;
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, login, sum(x) AS s FROM t_dist_04402 GROUP BY date, login HAVING date = '2024-01-05' AND s > 63
);

-- `grouping` and `arrayJoin` conjuncts stay in HAVING: `grouping` is not allowed in WHERE, and
-- `arrayJoin` would multiply the rows before aggregation.
SELECT '--- HAVING with grouping and arrayJoin';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date = '2024-01-05' AND grouping(date) = 0;
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING arrayJoin([date, date]) = '2024-01-05';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING unnest([date, date]) = '2024-01-05';

-- `indexHint` conjuncts stay in HAVING: there the hint is always true, while before aggregation
-- it would prune the other partitions.
SELECT '--- HAVING with indexHint';
SELECT count(), sum(s) FROM (SELECT date, sum(x) AS s FROM t_dist_04402 GROUP BY date HAVING indexHint(date = '2024-01-05'));
SELECT count(), sum(s) FROM (SELECT date, sum(x) AS s FROM t_dist_04402 GROUP BY date HAVING indexHint(date = '2024-01-05') OR date = '2024-01-06');

-- A conjunct with a deterministic subquery is moved; with a non-deterministic subquery it stays in HAVING,
-- because a correlated subquery in WHERE would be evaluated per source row instead of per group.
SELECT '--- HAVING with deterministic subquery';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date IN (SELECT toDate('2024-01-05'));
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date IN (SELECT toDate('2024-01-05'))
);
SELECT '--- HAVING with non-deterministic subquery';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date IN (SELECT toDate('2024-01-05') + rand() % 1);
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date IN (SELECT toDate('2024-01-05') + rand() % 1)
);
SELECT '--- HAVING with stateful function in subquery';
-- `timeSeriesStoreTags` is deterministic, but has a side effect, so the conjunct stays in HAVING.
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date
    HAVING date IN (SELECT toDate('2024-01-05') + timeSeriesStoreTags(0, [('a', 'b')], '__name__', 'm'));
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date
        HAVING date IN (SELECT toDate('2024-01-05') + timeSeriesStoreTags(0, [('a', 'b')], '__name__', 'm'))
);

-- Conjuncts whose result may differ between servers stay in HAVING, because HAVING is executed on the initiator:
-- functions like `shardNum`, and subqueries reading a table or a table function.
SELECT '--- HAVING with shardNum';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date >= toDate('2024-01-01') + 5 * shardNum() ORDER BY date;
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date >= toDate('2024-01-01') + 5 * shardNum()
);
SELECT '--- HAVING with subquery reading a table';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date IN (SELECT date FROM t_dates_04402);
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date IN (SELECT date FROM t_dates_04402)
);
SELECT '--- HAVING with subquery reading a table function';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date
    HAVING date IN (SELECT toDate('2024-01-05') + x % 1 FROM generateRandom('x UInt8') LIMIT 1);
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date
        HAVING date IN (SELECT toDate('2024-01-05') + x % 1 FROM generateRandom('x UInt8') LIMIT 1)
);

-- The set of `GLOBAL IN` is computed on the initiator and sent to every shard, so the conjunct is moved to the shard `WHERE`.
SELECT '--- HAVING with GLOBAL IN subquery reading a table';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date GLOBAL IN (SELECT date FROM t_dates_04402);
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date GLOBAL IN (SELECT date FROM t_dates_04402)
);
SELECT '--- HAVING with GLOBAL NOT IN subquery reading a table function';
SELECT count() FROM (
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date
        HAVING date GLOBAL NOT IN (SELECT toDate('2024-01-05') + number FROM numbers(1))
);
SELECT
    countIf(explain ILIKE '%Condition:%date%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date
        HAVING date GLOBAL NOT IN (SELECT toDate('2024-01-05') + number FROM numbers(1))
);

-- The predicate is not moved for ROLLUP and TOTALS, where it would change the super-aggregate rows.
SELECT '--- WITH ROLLUP';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date WITH ROLLUP HAVING date = '2024-01-05' ORDER BY date;
SELECT '--- WITH TOTALS';
SELECT date, sum(x) FROM t_dist_04402 GROUP BY date WITH TOTALS HAVING date = '2024-01-05' ORDER BY date
    SETTINGS totals_mode = 'after_having_exclusive';

-- Complete-stage remote(): the shard executes HAVING itself; an outer filter on an aggregate alias
-- must stay in HAVING.
SELECT '--- complete-stage remote() with outer filter on aggregate alias';
SELECT date, s FROM (
    SELECT date, sum(x) AS s FROM remote('127.0.0.2', currentDatabase(), t_local_04402) GROUP BY date
) WHERE s > 30000 ORDER BY date;
SELECT '--- complete-stage remote() with outer filter on grouping key';
SELECT date, s FROM (
    SELECT date, sum(x) AS s FROM remote('127.0.0.2', currentDatabase(), t_local_04402) GROUP BY date
) WHERE date = '2024-01-05' ORDER BY date;

-- After-aggregation stage: the shard executes HAVING and pushes it below the aggregation by itself.
SELECT '--- distributed_group_by_no_merge = 2';
SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
    SETTINGS distributed_group_by_no_merge = 2;
SELECT
    countIf(explain ILIKE '%Condition: (date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
)
SETTINGS distributed_group_by_no_merge = 2;

-- Without GROUP BY there are no aggregation keys: a constant HAVING must not become a shard WHERE.
SELECT '--- no GROUP BY, constant HAVING';
SELECT sum(x) FROM t_dist_04402 HAVING 0;
SELECT sum(x) FROM t_dist_04402 HAVING 1;

-- `group_by_use_nulls` makes the keys `Nullable` only with `ROLLUP`, `CUBE` and `GROUPING SETS`, which are not rewritten.
-- With a plain `GROUP BY` the keys keep their type, so a type-sensitive grouping-key conjunct can still move to the shard `WHERE`.
SELECT '--- group_by_use_nulls = 1';
SELECT date, toTypeName(date), sum(x) FROM t_dist_04402 GROUP BY date HAVING date = '2024-01-05' AND toTypeName(date) = 'Date'
    SETTINGS group_by_use_nulls = 1;
SELECT count() FROM (SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING toTypeName(date) = 'Nullable(Date)')
    SETTINGS group_by_use_nulls = 1;
SELECT
    countIf(explain ILIKE '%Condition: (date in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(x) FROM t_dist_04402 GROUP BY date HAVING date = '2024-01-05'
)
SETTINGS group_by_use_nulls = 1;

-- A function with observable side effects, like `sleepEachRow`, must stay in `HAVING`: it runs once per group,
-- not once per source row. Ten rows of one group need 10000 microseconds per block, above the limit of 2000.
SELECT '--- HAVING with sleepEachRow';
DROP TABLE IF EXISTS t_sleep_local_04402;
DROP TABLE IF EXISTS t_sleep_dist_04402;
CREATE TABLE t_sleep_local_04402 (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_sleep_dist_04402 AS t_sleep_local_04402
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_sleep_local_04402');
INSERT INTO t_sleep_local_04402 SELECT 1, number FROM numbers(10);
SELECT k, sum(x) FROM t_sleep_dist_04402 GROUP BY k HAVING sleepEachRow(0.001) = 0
    SETTINGS query_plan_enable_optimizations = 0, max_block_size = 16, function_sleep_max_microseconds_per_block = 2000;

-- A grouping-key conjunct must not be moved to `WHERE` while a sibling with observable side effects stays in `HAVING`:
-- that would remove groups before `sleepEachRow` is evaluated. Ten groups need 10000 microseconds per block.
-- Two-level aggregation would split the groups into several blocks.
SELECT '--- HAVING with sleepEachRow and a grouping-key conjunct';
TRUNCATE TABLE t_sleep_local_04402;
INSERT INTO t_sleep_local_04402 SELECT number % 10, 1 FROM numbers(100);
SELECT k, count() FROM t_sleep_dist_04402 GROUP BY k HAVING sleepEachRow(0.001) = 0 AND k = 0
    SETTINGS query_plan_enable_optimizations = 0, short_circuit_function_evaluation = 'disable',
        max_threads = 1, max_block_size = 100, function_sleep_max_microseconds_per_block = 2000,
        group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0; -- { serverError TOO_SLOW }
DROP TABLE t_sleep_dist_04402;
DROP TABLE t_sleep_local_04402;

-- A conjunct depending on the time zone of the server stays in `HAVING`, because the shards may have a different
-- time zone than the initiator. With an explicit time zone it is moved and used for the primary key analysis.
SELECT '--- HAVING depending on the time zone of the server';
-- `DateTime` without a time zone takes the session time zone when the table is created and when the query is analyzed.
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS t_tz_local_04402;
DROP TABLE IF EXISTS t_tz_dist_04402;
CREATE TABLE t_tz_local_04402 (dt DateTime, dt_utc DateTime('UTC'), x UInt64)
    ENGINE = MergeTree ORDER BY (dt, dt_utc) SETTINGS index_granularity = 10;
CREATE TABLE t_tz_dist_04402 AS t_tz_local_04402
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_tz_local_04402');
INSERT INTO t_tz_local_04402 SELECT toDateTime(number * 3600, 'UTC'), toDateTime(number * 3600, 'UTC'), 1 FROM numbers(1000);
SELECT count() FROM (SELECT dt, sum(x) FROM t_tz_dist_04402 GROUP BY dt HAVING toHour(dt) = 0);
SELECT count() FROM (SELECT dt_utc, sum(x) FROM t_tz_dist_04402 GROUP BY dt_utc HAVING toHour(dt_utc) = 0);
SELECT
    countIf(explain ILIKE '%Condition: (dt in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_parts
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT dt, sum(x) FROM t_tz_dist_04402 GROUP BY dt HAVING dt < '1970-01-02 00:00:00'
);
SELECT
    countIf(explain ILIKE '%Condition: (dt_utc in%') > 0 AS shard_prunes,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_parts
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT dt_utc, sum(x) FROM t_tz_dist_04402 GROUP BY dt_utc HAVING dt_utc < toDateTime(86400, 'UTC')
);
SELECT
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_parts
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT dt_utc, sum(x) FROM t_tz_dist_04402 GROUP BY dt_utc HAVING toUnixTimestamp(toString(dt_utc)) < 86400
);
DROP TABLE t_tz_dist_04402;
DROP TABLE t_tz_local_04402;

DROP VIEW v_hav_04402;
DROP VIEW v_agg_04402;
DROP TABLE t_dist_04402;
DROP TABLE t_local_04402;
DROP TABLE t_dates_04402;
