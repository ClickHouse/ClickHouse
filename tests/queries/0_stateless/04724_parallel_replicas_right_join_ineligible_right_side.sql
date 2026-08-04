DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_log;

CREATE TABLE t_left (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_left SELECT number FROM numbers(10);

CREATE TABLE t_right (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_right SELECT number FROM numbers(10);

CREATE TABLE t_log (key UInt64) ENGINE = Log;
INSERT INTO t_log SELECT number FROM numbers(10);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_prefer_local_join = 0;

-- The left side of a RIGHT JOIN is materialized into a temporary table, so parallel replicas
-- must be rejected when the right side has no eligible table. Previously the decision walk
-- validated the left side and the query then failed with a logical error.

SELECT '-- right side is a table function';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- right side is a Log table';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_log) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- RIGHT ANY JOIN';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT ANY JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- right side is system.one';
SELECT r.key FROM (SELECT key FROM t_left) AS l
RIGHT JOIN (SELECT dummy AS key FROM system.one) AS r ON l.key = r.key
ORDER BY r.key;

-- The optimization must still be applied when the right side does hold an eligible table.
-- A `GLOBAL ALL RIGHT JOIN` in the remote query marks the whole JOIN offloaded to the
-- replicas; when the decision is rejected only the inner right subquery is read remotely.

SELECT '-- left ineligible, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

SELECT '-- left Log, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_log) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

SELECT '-- both sides eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_left) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

SELECT '-- results are correct when the JOIN is offloaded';
SELECT r.key FROM (SELECT number AS key FROM numbers(5)) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_log;
