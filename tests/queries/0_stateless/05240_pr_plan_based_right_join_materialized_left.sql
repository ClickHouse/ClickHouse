-- A `RIGHT JOIN` whose left side cannot be read with parallel replicas is handled differently by
-- the two implementations, and this test pins the plan-based half.
--
-- Query-based materializes such a left side into a temporary table and ships the whole join, so the
-- join runs on the replicas and the initiator plan holds a single `ReadFromRemoteParallelReplicas`
-- step whose description is the shipped SQL. That is what
-- `04724_parallel_replicas_right_join_materialized_left_*` asserts, and it cannot be shared with
-- plan-based, which ships a serialized plan fragment rather than SQL.
--
-- Plan-based ships the join itself when the left side is shippable - a `View` or a materialized view
-- over `MergeTree` - and then the fragment contains `JoinLogical`. When the left side is not
-- shippable - a `Memory` table, say - only the right side's read is
-- distributed and the join runs on the initiator. Both cases answer correctly, so the difference
-- costs parallelism rather than correctness.
--
-- The counts are read from the shipped fragment rather than from the initiator's own plan, because
-- with `parallel_replicas_local_plan = 1` the initiator also holds the local replica's branch of the
-- union, which has a join of its own.

DROP TABLE IF EXISTS t_right SYNC;
DROP TABLE IF EXISTS t_left SYNC;
DROP TABLE IF EXISTS t_mem;
DROP VIEW IF EXISTS v_left;
DROP TABLE IF EXISTS mv_left SYNC;

CREATE TABLE t_right (key UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_right', 'r1') ORDER BY key;
INSERT INTO t_right SELECT number FROM numbers(10);

CREATE TABLE t_left (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_left SELECT number FROM numbers(5);

CREATE TABLE t_mem (key UInt64) ENGINE = Memory;
INSERT INTO t_mem SELECT number FROM numbers(5);

CREATE VIEW v_left AS SELECT key FROM t_left;

CREATE MATERIALIZED VIEW mv_left ENGINE = MergeTree ORDER BY key AS SELECT key FROM t_left;
INSERT INTO t_left SELECT number FROM numbers(5, 3);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1;
SET explain_query_plan_default = 'legacy';
SET join_algorithm = 'hash', join_use_nulls = 0;

SELECT 'non-MergeTree left: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM t_mem AS l RIGHT JOIN t_right AS r ON l.key = r.key);
SELECT count(), sum(r.key) FROM t_mem AS l RIGHT JOIN t_right AS r ON l.key = r.key;
SELECT 'the join is shipped to the replicas';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM t_mem AS l RIGHT JOIN t_right AS r ON l.key = r.key);

SELECT 'view left: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM v_left AS l RIGHT JOIN t_right AS r ON l.key = r.key);
SELECT count(), sum(r.key) FROM v_left AS l RIGHT JOIN t_right AS r ON l.key = r.key;
SELECT 'the join is shipped to the replicas';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM v_left AS l RIGHT JOIN t_right AS r ON l.key = r.key);

SELECT 'materialized view left: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM mv_left AS l RIGHT JOIN t_right AS r ON l.key = r.key);
SELECT count(), sum(r.key) FROM mv_left AS l RIGHT JOIN t_right AS r ON l.key = r.key;
SELECT 'the join is shipped to the replicas';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM mv_left AS l RIGHT JOIN t_right AS r ON l.key = r.key);

DROP TABLE mv_left SYNC;
DROP VIEW v_left;
DROP TABLE t_mem;
DROP TABLE t_left SYNC;
DROP TABLE t_right SYNC;
