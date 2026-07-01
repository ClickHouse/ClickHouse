-- Tags: shard, no-parallel-replicas, no-random-merge-tree-settings
-- Regression test for issue #108284.

set enable_analyzer = 1;
-- Force both shards through the remote path so the plan we inspect is the one sent to the shard,
-- not the initiator-planned local replica (which always prunes regardless of the pushdown).
set prefer_localhost_replica = 0;
-- With a serialized query plan the shard receives a plan (not SQL) and EXPLAIN distributed=1 cannot
-- introspect its index usage; pin it off so the shard-side pruning is visible.
set serialize_query_plan = 0;
-- optimize_skip_unused_shards prunes the shard query on the initiator independently of the
-- predicate pushdown, so the shard index Condition would show pruning even when the pushdown is
-- disabled; pin it off so the assertion reflects only the enable_optimize_predicate_expression move.
set optimize_skip_unused_shards = 0;

DROP TABLE IF EXISTS t_local_04402;
DROP TABLE IF EXISTS t_dist_04402;
DROP VIEW IF EXISTS v_agg_04402;

CREATE TABLE t_local_04402 (date Date, login UInt64, x Float64)
    ENGINE = MergeTree PARTITION BY date ORDER BY (date, login);

CREATE TABLE t_dist_04402 AS t_local_04402
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_local_04402', sipHash64(login));

-- Aggregating view over the Distributed table; 'date' is a GROUP BY key (and partition key).
CREATE VIEW v_agg_04402 AS SELECT date, login, sum(x) AS s FROM t_dist_04402 GROUP BY date, login;

INSERT INTO t_local_04402
    SELECT toDate('2024-01-01') + (number % 10), number % 1000, number % 7
    FROM numbers(100000);

-- The result must be identical whether or not the predicate is pushed down: only pruning changes.
SELECT '--- result (enable_optimize_predicate_expression = 1)';
SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
    SETTINGS enable_optimize_predicate_expression = 1;
SELECT '--- result (enable_optimize_predicate_expression = 0)';
SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
    SETTINGS enable_optimize_predicate_expression = 0;

-- enable_optimize_predicate_expression = 1 (default): the grouping-key predicate is moved to a
-- pre-aggregation WHERE on the shard, so the shard prunes partitions/index (Condition on 'date').
SELECT '--- shard pruning (enable_optimize_predicate_expression = 1)';
SELECT
    countIf(explain ILIKE '%Condition: (date in%') > 0 AS predicate_pushed_to_shard,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
)
SETTINGS enable_optimize_predicate_expression = 1;

-- enable_optimize_predicate_expression = 0: pushdown disabled, the predicate stays a post-aggregation
-- HAVING deferred to the initiator, so the shard reads every partition (no pruning).
SELECT '--- shard pruning (enable_optimize_predicate_expression = 0)';
SELECT
    countIf(explain ILIKE '%Condition: (date in%') > 0 AS predicate_pushed_to_shard,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_agg_04402 WHERE date = '2024-01-05' GROUP BY date
)
SETTINGS enable_optimize_predicate_expression = 0;

-- Carrier variant: the grouping-key predicate lives in the aggregating view's own HAVING
-- (not an outer WHERE). filterPushDown turns that HAVING into a filter pushed into ReadFromRemote,
-- so the same move applies and the shard prunes, matching the old analyzer.
CREATE VIEW v_hav_04402 AS
    SELECT date, login, sum(x) AS s FROM t_dist_04402 GROUP BY date, login HAVING date = '2024-01-05';

SELECT '--- inner-HAVING result (enable_optimize_predicate_expression = 1)';
SELECT date, sum(s) FROM v_hav_04402 GROUP BY date SETTINGS enable_optimize_predicate_expression = 1;
SELECT '--- inner-HAVING result (enable_optimize_predicate_expression = 0)';
SELECT date, sum(s) FROM v_hav_04402 GROUP BY date SETTINGS enable_optimize_predicate_expression = 0;

-- Same assertion as above, but the shard Condition here is a conjunction (the pushed filter and
-- the view's own HAVING conjunct both reach the index), so match the grouping-key term loosely.
SELECT '--- inner-HAVING shard pruning (enable_optimize_predicate_expression = 1)';
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS predicate_pushed_to_shard,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_hav_04402 GROUP BY date
)
SETTINGS enable_optimize_predicate_expression = 1;

SELECT '--- inner-HAVING shard pruning (enable_optimize_predicate_expression = 0)';
SELECT
    countIf(explain ILIKE '%Condition:%date in%') > 0 AS predicate_pushed_to_shard,
    countIf(explain ILIKE '%Condition: true%') > 0 AS shard_reads_all_partitions
FROM (
    EXPLAIN indexes = 1, distributed = 1
    SELECT date, sum(s) FROM v_hav_04402 GROUP BY date
)
SETTINGS enable_optimize_predicate_expression = 0;

DROP VIEW v_hav_04402;
DROP VIEW v_agg_04402;
DROP TABLE t_dist_04402;
DROP TABLE t_local_04402;
