-- Tags: distributed
-- The `has()` -> `IN` rewrite over a scalar subquery array has to name the set identically on the
-- initiator and on every secondary server. An initiator keeps the array as a `__getScalar` reference,
-- while a secondary server folds that reference into a constant and the AST sent to the next hop
-- carries the value, so a name derived from the node instead of the value makes the initiator ask for
-- a column the shard did not produce (THERE_IS_NO_COLUMN / NOT_FOUND_COLUMN_IN_BLOCK).
-- This is only observable when the predicate's value crosses the network, which is why 05227's
-- `WHERE`-only distributed coverage does not see it.

SET enable_analyzer = 1;
SET optimize_rewrite_has_to_in = 1;
SET enable_scalar_subquery_optimization = 1;
SET rewrite_in_to_join = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
-- Every leg has to be a real remote connection: a local replica plans the query once and so cannot
-- disagree with itself.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_05228_local;
DROP TABLE IF EXISTS t_05228_dist;
DROP TABLE IF EXISTS t_05228_dist_over_dist;

CREATE TABLE t_05228_local (type UInt32, uid LowCardinality(String), uids String) ENGINE = MergeTree ORDER BY type;
INSERT INTO t_05228_local SELECT 1, toString(number % 5), toString(number % 5) FROM numbers(20);

CREATE TABLE t_05228_dist AS t_05228_local
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05228_local);
CREATE TABLE t_05228_dist_over_dist AS t_05228_local
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05228_dist);

-- The predicate is projected and then grouped, so its value has to be in the block the shard returns.
SELECT 'projected lowcardinality needle', has((SELECT groupUniqArray(uid) FROM t_05228_local), uid) AS p, count()
FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local) GROUP BY p ORDER BY p;

-- The same with a plain String needle: the divergence is not specific to the LowCardinality cast.
SELECT 'projected string needle', has((SELECT groupUniqArray(uids) FROM t_05228_local), uids) AS p, count()
FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local) GROUP BY p ORDER BY p;

SELECT 'order by predicate', uid FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local)
ORDER BY has((SELECT groupUniqArray(uid) FROM t_05228_local), uid) DESC, uid ASC LIMIT 3;

SELECT DISTINCT 'window over predicate', sum(has((SELECT groupUniqArray(uid) FROM t_05228_local), uid)) OVER () AS s
FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local);

SELECT 'parallel replicas', has((SELECT groupUniqArray(uid) FROM t_05228_local), uid) AS p, count()
FROM t_05228_local GROUP BY p ORDER BY p
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_local_plan = 0,
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'distributed table', has((SELECT groupUniqArray(uid) FROM t_05228_local), uid) AS p, count()
FROM t_05228_dist GROUP BY p ORDER BY p;

-- Two hops: the middle server both receives the value and sends it on, so all three have to agree.
SELECT 'distributed over distributed', has((SELECT groupUniqArray(uid) FROM t_05228_local), uid) AS p, count()
FROM t_05228_dist_over_dist GROUP BY p ORDER BY p;

-- Above the size at which a secondary server stops folding the shipped set source, the rewrite is
-- declined, so both sides keep `has()`.
SELECT 'above fold cutoff', has((SELECT groupUniqArray(repeat(toString(number), 200)) FROM numbers(1800)), repeat(uids, 200)) AS p, count()
FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local) GROUP BY p ORDER BY p;

-- That count is the same whether or not the rewrite fires, so it discriminates only while an
-- unguarded rewrite errors. These two pin the shape itself: the predicate above the cutoff stays
-- `has`, and the same predicate below it does not, so declining every size would fail the second row.
SELECT 'above cutoff keeps has', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT has((SELECT groupUniqArray(repeat(toString(number), 200)) FROM numbers(1800)), repeat(uids, 200)) AS p, count()
    FROM t_05228_local GROUP BY p
    ) WHERE explain ILIKE '%function_name: has%';
SELECT 'below cutoff keeps has', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT has((SELECT groupUniqArray(repeat(toString(number), 200)) FROM numbers(1200)), repeat(uids, 200)) AS p, count()
    FROM t_05228_local GROUP BY p
    ) WHERE explain ILIKE '%function_name: has%';

-- Controls: a literal array (the same node on both sides) and a plain tuple `IN` (not rewritten at
-- all) have to keep answering in the same shape.
SELECT 'literal array control', has(['0', '1', '2'], uid) AS p, count()
FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local) GROUP BY p ORDER BY p;

SELECT 'tuple in control', uid IN ('0', '1', '2') AS p, count()
FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_05228_local) GROUP BY p ORDER BY p;

-- The rewrite still fires below the cutoff: a "fix" that just declined would pass everything above.
SELECT 'rewrite fires', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT has((SELECT groupUniqArray(uid) FROM t_05228_local), uid) AS p, count() FROM t_05228_local GROUP BY p
    ) WHERE explain ILIKE '%function_name: in%';

DROP TABLE t_05228_dist_over_dist;
DROP TABLE t_05228_dist;
DROP TABLE t_05228_local;
