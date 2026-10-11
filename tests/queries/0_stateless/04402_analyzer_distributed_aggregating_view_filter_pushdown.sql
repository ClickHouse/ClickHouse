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

DROP VIEW v_hav_04402;
DROP VIEW v_agg_04402;
DROP TABLE t_dist_04402;
DROP TABLE t_local_04402;
DROP TABLE t_dates_04402;
