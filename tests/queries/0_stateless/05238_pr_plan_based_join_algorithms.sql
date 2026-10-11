-- The plan-based counterpart of the `02967_parallel_replicas_join_algo_and_analyzer_*` and
-- `03255_parallel_replicas_join_algo_and_analyzer_4` family. Those assert the query text shipped to
-- the replicas, which only the query-based implementation produces, so they are pinned to it; this
-- test keeps the same ground covered for plan-based by asserting what is implementation-neutral:
-- the join is distributed, and the answer matches a run without parallel replicas.
--
-- The dimensions are the ones that family varies: the join algorithm (`hash` and
-- `full_sorting_merge`) and `parallel_replicas_prefer_local_join`, over a LEFT and a RIGHT join.

DROP TABLE IF EXISTS t_algo_left SYNC;
DROP TABLE IF EXISTS t_algo_right SYNC;

CREATE TABLE t_algo_left (item_id UInt64, price UInt64) ENGINE = MergeTree ORDER BY item_id;
CREATE TABLE t_algo_right (item_id UInt64) ENGINE = MergeTree ORDER BY item_id;

INSERT INTO t_algo_left SELECT number, number % 10 FROM numbers(1000);
INSERT INTO t_algo_right SELECT number FROM numbers(0, 1000, 2);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1;
SET explain_query_plan_default = 'legacy';

-- The reference value each distributed run has to reproduce.
SELECT 'truth';
SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS enable_parallel_replicas = 0;
SELECT count(), sum(r.item_id) FROM t_algo_left AS l RIGHT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS enable_parallel_replicas = 0;

SELECT 'hash, prefer_local_join=0: distributed, then the answer';
SELECT countIf(explain ILIKE '%ParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id)
SETTINGS join_algorithm = 'hash', parallel_replicas_prefer_local_join = 0;
SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS join_algorithm = 'hash', parallel_replicas_prefer_local_join = 0;

SELECT 'hash, prefer_local_join=1: distributed, then the answer';
SELECT countIf(explain ILIKE '%ParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id)
SETTINGS join_algorithm = 'hash', parallel_replicas_prefer_local_join = 1;
SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS join_algorithm = 'hash', parallel_replicas_prefer_local_join = 1;

SELECT 'full_sorting_merge, prefer_local_join=0: distributed, then the answer';
SELECT countIf(explain ILIKE '%ParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id)
SETTINGS join_algorithm = 'full_sorting_merge', parallel_replicas_prefer_local_join = 0;
SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS join_algorithm = 'full_sorting_merge', parallel_replicas_prefer_local_join = 0;

SELECT 'full_sorting_merge, prefer_local_join=1: distributed, then the answer';
SELECT countIf(explain ILIKE '%ParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id)
SETTINGS join_algorithm = 'full_sorting_merge', parallel_replicas_prefer_local_join = 1;
SELECT count(), sum(l.price) FROM t_algo_left AS l LEFT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS join_algorithm = 'full_sorting_merge', parallel_replicas_prefer_local_join = 1;

SELECT 'right join, hash: distributed, then the answer';
SELECT countIf(explain ILIKE '%ParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT count(), sum(r.item_id) FROM t_algo_left AS l RIGHT JOIN t_algo_right AS r ON l.item_id = r.item_id)
SETTINGS join_algorithm = 'hash', parallel_replicas_prefer_local_join = 0;
SELECT count(), sum(r.item_id) FROM t_algo_left AS l RIGHT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS join_algorithm = 'hash', parallel_replicas_prefer_local_join = 0;

SELECT 'right join, full_sorting_merge: distributed, then the answer';
SELECT countIf(explain ILIKE '%ParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT count(), sum(r.item_id) FROM t_algo_left AS l RIGHT JOIN t_algo_right AS r ON l.item_id = r.item_id)
SETTINGS join_algorithm = 'full_sorting_merge', parallel_replicas_prefer_local_join = 0;
SELECT count(), sum(r.item_id) FROM t_algo_left AS l RIGHT JOIN t_algo_right AS r ON l.item_id = r.item_id
SETTINGS join_algorithm = 'full_sorting_merge', parallel_replicas_prefer_local_join = 0;

DROP TABLE t_algo_left SYNC;
DROP TABLE t_algo_right SYNC;
