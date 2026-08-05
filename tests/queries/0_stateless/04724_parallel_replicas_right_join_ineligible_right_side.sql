DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_mid;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_left SELECT number FROM numbers(10);

CREATE TABLE t_mid (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_mid SELECT number FROM numbers(10);

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

-- For a RIGHT JOIN the left side is materialized into a temporary table, so only the right side
-- is read with replicas and it is the side the eligibility decision has to look at. Descending
-- the left side instead accepted a query whose right side has no eligible table, and the table
-- lookup on the rewritten tree then failed with a logical error.

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

SELECT '-- RIGHT SEMI JOIN';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT SEMI JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- RIGHT ANTI JOIN';
SELECT r.key FROM (SELECT key FROM t_left WHERE key < 5) AS l
RIGHT ANTI JOIN (SELECT number AS key FROM numbers(10)) AS r ON l.key = r.key
ORDER BY r.key;

-- A RIGHT JOIN inside the remote query marks the whole JOIN offloaded to the replicas; when the
-- decision is rejected only the right subquery is read remotely. The offload has to survive for
-- every shape whose right side holds an eligible table, whatever the left side reads.

SELECT '-- left is a table function, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left is a table function, right eligible: JOIN is offloaded with a local join';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left is system.one, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT dummy AS key FROM system.one) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left is a table function, right eligible: results are correct';
SELECT r.key FROM (SELECT number AS key FROM numbers(5)) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- left is a table function, right eligible: results are correct with a local join';
SELECT r.key FROM (SELECT number AS key FROM numbers(5)) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_prefer_local_join = 1;

-- The left side may itself be a join, and which of its own sides reads the table function must
-- not matter: the whole left side is materialized either way.

SELECT '-- left joins a table function to a table, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT b.key AS key FROM numbers(10) AS a LEFT JOIN t_mid AS b ON a.number = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left joins a table function to a table, right eligible: JOIN is offloaded with a local join';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT b.key AS key FROM numbers(10) AS a LEFT JOIN t_mid AS b ON a.number = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left joins a table to a table function, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT a.key AS key FROM t_mid AS a LEFT JOIN numbers(10) AS b ON a.key = b.number
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left joins a table function to a table, right eligible: results are correct';
SELECT r.key FROM (
    SELECT b.key AS key FROM numbers(5) AS a LEFT JOIN t_mid AS b ON a.number = b.key
) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

SELECT '-- left joins a table to a table function, right eligible: results are correct';
SELECT r.key FROM (
    SELECT a.key AS key FROM t_mid AS a LEFT JOIN numbers(5) AS b ON a.key = b.number
) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

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

SELECT '-- left joins two tables, right eligible: JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT b.key AS key FROM t_left AS a LEFT JOIN t_mid AS b ON a.key = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- left joins two tables, right eligible: results are correct';
SELECT r.key FROM (
    SELECT b.key AS key FROM t_left AS a LEFT JOIN t_mid AS b ON a.key = b.key WHERE b.key < 5
) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key;

-- Now that a RIGHT JOIN can be offloaded, its left side is the one materialized into a temporary
-- table before the query is sent to the replicas. A left side that is not a MergeTree has to force
-- a global join, otherwise every replica would read its own copy of it and produce a different
-- result. Merge is used here because the stress runner rewrites Log/TinyLog/StripeLog/Memory to
-- MergeTree, which would silently turn these assertions into controls.

CREATE TABLE t_merge_left (key UInt64) ENGINE = Merge(currentDatabase(), '^t_mid$');

-- The assertions below key on GLOBAL ALL RIGHT JOIN, which encodes orientation, so they pin
-- query_plan_join_swap_table rather than take the value the test runner randomizes.

SELECT '-- non-MergeTree left, right eligible: left is materialized into a temporary table';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_merge_left) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- non-MergeTree left, right eligible: the raw left read is not sent to the replicas';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_merge_left) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%t_merge_left%';

SELECT '-- non-MergeTree left, right eligible: results are correct';
SELECT r.key FROM (SELECT key FROM t_merge_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_prefer_local_join = 1;

-- A table function on the left side needs the same treatment. It is not one of the four names
-- CollectStoragesVisitor collects a storage for, so leaving it out would make the side read as
-- having no storages at all, hence vacuously all-MergeTree.

SELECT '-- table function left, right eligible: left is materialized into a temporary table';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- table function left, right eligible: the raw left read is not sent to the replicas';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number AS key FROM numbers(10)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%numbers(%';

-- Being a MergeTree is not on its own enough for the materialized side to be read by each replica
-- separately: it also has to be eligible for parallel replicas. The arms below therefore run at the
-- DEFAULT parallel_replicas_for_non_replicated_merge_tree = 0, where a plain MergeTree is not, so
-- the right side needs to be replicated to keep the JOIN offloaded at all.

CREATE TABLE t_plain_left (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_plain_left SELECT number FROM numbers(10);

CREATE TABLE t_replicated_right (key UInt64)
ENGINE = ReplicatedMergeTree('/parallel_replicas/{database}/t_replicated_right', 'r1') ORDER BY key;
INSERT INTO t_replicated_right SELECT number FROM numbers(10);

SELECT '-- non-replicated MergeTree left, right eligible: left is materialized into a temporary table';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_plain_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- non-replicated MergeTree left, right eligible: the raw left read is not sent to the replicas';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_plain_left) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%t_plain_left%';

SELECT '-- non-replicated MergeTree left, right eligible: results are correct';
SELECT r.key FROM (SELECT key FROM t_plain_left WHERE key < 5) AS l
RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1;

-- A replicated left side is eligible, so it keeps the local join the optimization exists for.

SELECT '-- replicated left, right eligible: the local join is kept';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_replicated_right) AS l
    RIGHT JOIN (SELECT key FROM t_replicated_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

DROP TABLE t_replicated_right SYNC;
DROP TABLE t_plain_left;
DROP TABLE t_merge_left;
DROP TABLE t_left;
DROP TABLE t_mid;
DROP TABLE t_right;
