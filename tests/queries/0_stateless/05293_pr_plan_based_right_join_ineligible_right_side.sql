-- The plan-based counterpart of `04724_parallel_replicas_right_join_ineligible_right_side`. That test
-- asserts the shipped SQL query - one `EXPLAIN` line carrying both `ReadFromRemoteParallelReplicas`
-- and `RIGHT JOIN` - which only the query-based implementation produces, so it is pinned to it. What
-- this test keeps is the part that is implementation-neutral: a `RIGHT JOIN` whose right side holds no
-- eligible table must still answer correctly, rather than fail while the rewritten tree is looked up,
-- and a shape whose right side is eligible must have its join distributed whatever the left side reads.

DROP TABLE IF EXISTS t_inel_left SYNC;
DROP TABLE IF EXISTS t_inel_mid SYNC;
DROP TABLE IF EXISTS t_inel_right SYNC;

CREATE TABLE t_inel_left (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_inel_left SELECT number FROM numbers(10);

CREATE TABLE t_inel_mid (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_inel_mid SELECT number FROM numbers(10);

CREATE TABLE t_inel_right (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_inel_right SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_prefer_local_join = 0,
    parallel_replicas_plan_based = 1;
SET explain_query_plan_default = 'legacy';

-- A right side that holds no eligible table: nothing can be distributed there, and the strictness
-- must not change the answer either.

SELECT '-- right side is a table function';
SELECT r.key FROM (SELECT key FROM t_inel_left WHERE key < 5) AS l
RIGHT JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- right side is system.one';
SELECT r.key FROM (SELECT key FROM t_inel_left) AS l
RIGHT JOIN (SELECT dummy AS key FROM system.one) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- RIGHT ANY JOIN';
SELECT r.key FROM (SELECT key FROM t_inel_left WHERE key < 5) AS l
RIGHT ANY JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- RIGHT SEMI JOIN';
SELECT r.key FROM (SELECT key FROM t_inel_left WHERE key < 5) AS l
RIGHT SEMI JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- RIGHT ANTI JOIN';
SELECT r.key FROM (SELECT key FROM t_inel_left WHERE key < 5) AS l
RIGHT ANTI JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

-- The right side is eligible and the left side is not. Query-based materializes such a left side and
-- ships the whole join; plan-based ships neither, distributing only the right side's read, and the
-- join runs on the initiator - `JoinLogical` is what says whether it was shipped, since the fragment
-- is printed in logical form while a join the initiator kept is already a physical `Join` step.
-- A shippable left side moves these to 1, which is what the last arm shows.

SELECT '-- left is a table function: only the right side is distributed';
SELECT countIf(explain ILIKE '%JoinLogical%'), countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key);

SELECT '-- the same with a local join';
SELECT countIf(explain ILIKE '%JoinLogical%'), countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1);

SELECT '-- left is system.one';
SELECT countIf(explain ILIKE '%JoinLogical%'), countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM (SELECT dummy AS key FROM system.one) AS l
    RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key);

SELECT '-- left joins a table function to a table';
SELECT countIf(explain ILIKE '%JoinLogical%'), countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM (
        SELECT b.key AS key FROM numbers(10) AS a LEFT JOIN t_inel_mid AS b ON a.number = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key);

SELECT '-- both sides eligible: the join is shipped';
SELECT countIf(explain ILIKE '%JoinLogical%'), countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_inel_left) AS l
    RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key);

-- Whether the join was shipped or not, the answer is the one a run without parallel replicas gives.

SELECT '-- rows: left is a table function, right eligible';
SELECT r.key FROM (SELECT number AS key FROM numbers(5)) AS l
RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- rows: left joins a table function to a table, right eligible';
SELECT r.key FROM (
    SELECT b.key AS key FROM numbers(5) AS a LEFT JOIN t_inel_mid AS b ON a.number = b.key
) AS l
RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- rows: both sides eligible';
SELECT r.key FROM (SELECT key FROM t_inel_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_inel_right) AS r ON l.key = r.key
ORDER BY r.key;

DROP TABLE t_inel_right SYNC;
DROP TABLE t_inel_mid SYNC;
DROP TABLE t_inel_left SYNC;
