-- A plan fragment is serialized before it is sent to a replica, so a join can only be shipped when
-- every step of its broadcast side is serializable. `ReadFromSystemNumbers` is serializable only for
-- a bounded, unfiltered read, which is what a replica can rebuild from the table's parameters alone:
--   - a filter prunes the generated domain, and shipping the read without it would generate the whole
--     domain on the replica;
--   - an unbounded read depends on that pruning to terminate at all.
-- In both of those cases the fragment stays local and only the right side's read is distributed,
-- which still answers correctly.

DROP TABLE IF EXISTS t_numbers_join SYNC;

CREATE TABLE t_numbers_join (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_numbers_join SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1;
SET explain_query_plan_default = 'legacy';

SELECT 'bounded numbers on the broadcast side: the join is shipped';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM numbers(5) AS l RIGHT JOIN t_numbers_join AS r ON l.number = r.key);
SELECT count(), sum(r.key) FROM numbers(5) AS l RIGHT JOIN t_numbers_join AS r ON l.number = r.key;

SELECT 'an offset range is shipped too';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM numbers(3, 5) AS l RIGHT JOIN t_numbers_join AS r ON l.number = r.key);
SELECT count(), sum(r.key) FROM numbers(3, 5) AS l RIGHT JOIN t_numbers_join AS r ON l.number = r.key;

SELECT 'a filtered read is not shipped';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count(), sum(r.key) FROM (SELECT number FROM numbers(20) WHERE number % 3 = 0) AS l
    RIGHT JOIN t_numbers_join AS r ON l.number = r.key);
SELECT count(), sum(r.key) FROM (SELECT number FROM numbers(20) WHERE number % 3 = 0) AS l
RIGHT JOIN t_numbers_join AS r ON l.number = r.key;

SELECT 'an unbounded read is not shipped';
SELECT countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT count() FROM (SELECT number FROM system.numbers LIMIT 5) AS l
    RIGHT JOIN t_numbers_join AS r ON l.number = r.key);
SELECT count() FROM (SELECT number FROM system.numbers LIMIT 5) AS l
RIGHT JOIN t_numbers_join AS r ON l.number = r.key;

DROP TABLE t_numbers_join SYNC;
