-- A plan fragment is serialized before it is sent to a replica, so a join can only be shipped when
-- every step of its broadcast side is serializable. `ReadFromSystemNumbers` rebuilds itself on the
-- replica from the table's parameters plus its source filter, so a bounded read ships whether or not
-- it is filtered. An unbounded one does not: nothing would bound what the replica generates.
--
-- Shipping the filter matters beyond correctness - the plan keeps its own filtering step either way.
-- The filter is what prunes the generated domain, so a read that yields one value out of a huge
-- domain must not turn into generating that whole domain on every replica; the `read_rows` check
-- below pins that.

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

SELECT 'a filtered read is shipped too';
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

-- The domain is large and the filter selects one value out of it: with the filter shipped the
-- replicas generate a handful of rows, without it they would generate the whole domain.
SELECT 'the shipped filter still prunes the domain';
SELECT count() FROM (SELECT number FROM numbers(10000000) WHERE number = 5) AS l
RIGHT JOIN t_numbers_join AS r ON l.number = r.key
SETTINGS log_comment = 'pr_plan_based_numbers_pruning_05215';

SYSTEM FLUSH LOGS query_log;

SELECT max(read_rows) < 100000 FROM system.query_log
WHERE log_comment = 'pr_plan_based_numbers_pruning_05215' AND type = 'QueryFinish'
  AND event_date >= yesterday() AND current_database = currentDatabase();

DROP TABLE t_numbers_join SYNC;
