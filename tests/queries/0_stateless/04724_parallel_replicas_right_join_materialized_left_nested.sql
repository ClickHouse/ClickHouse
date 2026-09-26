DROP TABLE IF EXISTS t_mid;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_merge_left;
DROP TABLE IF EXISTS t_replicated_right;

CREATE TABLE t_mid (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_mid SELECT number FROM numbers(10);

CREATE TABLE t_right (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_right SELECT number FROM numbers(10);

-- The first group below runs at parallel_replicas_for_non_replicated_merge_tree = 0, where
-- a plain MergeTree is not eligible, so the table its nested join reads has to be replicated
-- for the outer JOIN to be offloaded at all.
CREATE TABLE t_replicated_right (key UInt64)
ENGINE = ReplicatedMergeTree('/parallel_replicas/{database}/t_replicated_right', 'r1') ORDER BY key;
INSERT INTO t_replicated_right SELECT number FROM numbers(10);

-- Merge is used for the ineligible table because the stress runner rewrites
-- Log/TinyLog/StripeLog/Memory to MergeTree, which would silently turn the assertions
-- below into controls.
CREATE TABLE t_merge_left (key UInt64) ENGINE = Merge(currentDatabase(), '^t_mid$');

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_prefer_local_join = 0;

-- The side read with replicas can itself hold a join whose own materialized side no replica may
-- read alone. That nested join is marked global too, so it must reach a temporary table: the
-- collector decides which child to descend and has to skip the same side that is materialized,
-- otherwise the nested global join keeps naming the raw table and every replica reads its own copy.
-- The absence of a raw nested global join is also what a query that was never offloaded, and one
-- whose nested join stayed local, would report, so it takes three arms: one requires the outer
-- JOIN to reach the replicas, one the nested join to name a temporary table.

SELECT '-- nested join in the offloaded right subtree: the outer JOIN is offloaded';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM (SELECT number AS key FROM numbers(10)) AS o
    RIGHT JOIN (
        SELECT a.key AS key FROM t_replicated_right AS a LEFT JOIN t_merge_left AS b ON a.key = b.key
    ) AS i ON o.key = i.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%RIGHT JOIN%';

SELECT '-- nested join in the offloaded right subtree: the inner side reaches a temporary table';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM (SELECT number AS key FROM numbers(10)) AS o
    RIGHT JOIN (
        SELECT a.key AS key FROM t_replicated_right AS a LEFT JOIN t_merge_left AS b ON a.key = b.key
    ) AS i ON o.key = i.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL LEFT JOIN%' AND explain NOT ILIKE '%GLOBAL ALL LEFT JOIN `_data_%';

SELECT '-- nested join in the offloaded right subtree: the temporary table is actually joined';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM (SELECT number AS key FROM numbers(10)) AS o
    RIGHT JOIN (
        SELECT a.key AS key FROM t_replicated_right AS a LEFT JOIN t_merge_left AS b ON a.key = b.key
    ) AS i ON o.key = i.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL LEFT JOIN `_data_%';

SELECT '-- nested join in the offloaded right subtree: results are correct';
SELECT count() FROM (SELECT number AS key FROM numbers(10)) AS o
RIGHT JOIN (
    SELECT a.key AS key FROM t_replicated_right AS a LEFT JOIN t_merge_left AS b ON a.key = b.key
) AS i ON o.key = i.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_prefer_local_join = 1;

-- A nested join written GLOBAL on the materialized side materializes its own side regardless of
-- what the outer JOIN decides, so the ineligible table under it is never read by a replica and
-- does not have to force the outer JOIN global. Only the locality distinguishes the two shapes:
-- without the keyword nothing else materializes that branch, which is the control below.

SELECT '-- explicit GLOBAL nested join on the materialized left: the local join is kept';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (
        SELECT a.key AS key FROM t_mid AS a GLOBAL LEFT JOIN t_merge_left AS b ON a.key = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%';

SELECT '-- explicit GLOBAL nested join on the materialized left: the nested side is materialized';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT a.key AS key FROM t_mid AS a GLOBAL LEFT JOIN t_merge_left AS b ON a.key = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL LEFT JOIN `_data_%';

SELECT '-- local nested join on the materialized left: the outer JOIN is still globalized';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT a.key AS key FROM t_mid AS a LEFT JOIN t_merge_left AS b ON a.key = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

-- Only one child of the nested global join reaches a temporary table. With the ineligible table on
-- the other one it is still read by every replica, so the outer JOIN has to be globalized after all.

SELECT '-- ineligible table on the surviving side of a nested global join: the outer JOIN is globalized';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (
        SELECT a.key AS key FROM t_merge_left AS a GLOBAL LEFT JOIN t_mid AS b ON a.key = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- ineligible table on the surviving side of a nested global join: the raw read is not sent to the replicas';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (
        SELECT a.key AS key FROM t_merge_left AS a GLOBAL LEFT JOIN t_mid AS b ON a.key = b.key
    ) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%' AND explain ILIKE '%t_merge_left%';

-- A `GLOBAL IN` on the materialized side is materialized by the same collector, so a storage
-- reachable only through one does not force the outer JOIN global either. A local `IN` is read by
-- every replica, which is the control.

SELECT '-- global IN on the materialized left: the local join is kept';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_mid WHERE key GLOBAL IN (SELECT key FROM t_merge_left)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain NOT ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- local IN on the materialized left: the outer JOIN is still globalized';
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM (SELECT key FROM t_mid WHERE key IN (SELECT key FROM t_merge_left)) AS l
    RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1,
             parallel_replicas_prefer_local_join = 1, query_plan_join_swap_table = 0
) WHERE explain ILIKE '%GLOBAL ALL RIGHT JOIN%' AND explain ILIKE '%_data_%';

SELECT '-- explicit GLOBAL nested join on the materialized left: results are correct';
SELECT r.key FROM (
    SELECT a.key AS key FROM t_mid AS a GLOBAL LEFT JOIN t_merge_left AS b ON a.key = b.key
) AS l
RIGHT JOIN (SELECT key FROM t_right) AS r ON l.key = r.key
ORDER BY r.key
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_prefer_local_join = 1;

DROP TABLE t_replicated_right SYNC;
DROP TABLE t_merge_left;
DROP TABLE t_mid;
DROP TABLE t_right;
