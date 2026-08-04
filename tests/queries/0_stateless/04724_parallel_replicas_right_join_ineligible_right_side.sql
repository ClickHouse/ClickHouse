DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_left SELECT number FROM numbers(10);

CREATE TABLE t_right (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_right SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_prefer_local_join = 0;

-- Only the right side of a RIGHT JOIN is read with replicas, so parallel replicas must be
-- rejected when the right side has no eligible table. Previously the decision walk validated
-- the left side instead and the query then failed with a logical error.

SELECT '-- right side is a table function';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- right side is system.one';
SELECT r.key FROM (SELECT key FROM t_left) AS l
RIGHT JOIN (SELECT dummy AS key FROM system.one) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- RIGHT ANY JOIN';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT ANY JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

-- An ineligible left side must be rejected as well, otherwise the whole JOIN is sent to every
-- replica with the left side still reading its own local rows, and the results are wrong.
-- A RIGHT JOIN inside the remote query marks the whole JOIN offloaded to the replicas; when the
-- decision is rejected only the right subquery is read remotely.

SELECT '-- left ineligible, right eligible: JOIN is not offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left system.one, right eligible: JOIN is not offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT dummy AS key FROM system.one) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left ineligible, right eligible: JOIN is not offloaded with a local join';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left ineligible, right eligible: results are correct';
SELECT r.key FROM (SELECT number AS key FROM numbers(5)) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- left ineligible, right eligible: results are correct with a local join';
SELECT r.key FROM (SELECT number AS key FROM numbers(5)) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_prefer_local_join = 1;

-- The optimization must still be applied when both sides hold an eligible table.

SELECT '-- both sides eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_left) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- both sides eligible: results are correct';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

DROP TABLE t_left;
DROP TABLE t_right;
