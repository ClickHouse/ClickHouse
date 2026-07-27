-- Parallel replicas must be disabled for an n-way join whose non-leftmost join is not replica-safe.
-- The whole join tree is shipped to every replica while only the leftmost leaf's reads are
-- coordinated, so such a join re-applies its own strictness independently on each replica and the
-- initiator concatenates the results, duplicating rows.

DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t1', 'r1') ORDER BY c;
CREATE TABLE t2 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t2', 'r1') ORDER BY c;
CREATE TABLE t3 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t3', 'r1') ORDER BY c;

INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (2), (3);
INSERT INTO t3 VALUES (7), (8);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
-- The plan-shape assertions below grep the legacy EXPLAIN step names; 'pretty' rewrites them.
SET explain_query_plan_default = 'legacy';

-- Mechanism: a non-replica-safe non-leftmost join must leave no remote-replicas read in the plan.
SELECT 'inner/any non-leftmost: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL);

SELECT 'inner/any non-leftmost under a left leftmost join: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 LEFT JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL);

-- An ARRAY JOIN also occupies the leftmost join-tree slot, so the join after it is non-leftmost too.
SELECT 'array join then inner/any: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a ANY INNER JOIN t3 ON 1 ORDER BY ALL);

-- Replica-safe join trees keep using parallel replicas.
SELECT 'array join then all/inner: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a INNER JOIN t2 ON t1.c = t2.c ORDER BY ALL);

-- An ALL-strictness join after an ARRAY JOIN stays eligible: the strictness is distributive, and a
-- non-distributive KIND is the business of the FULL/GLOBAL/CROSS rule, which cannot fire here
-- because the ARRAY JOIN does not increment `joins_count`.
SELECT 'array join then full: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a FULL JOIN t2 ON t1.c = t2.c ORDER BY ALL);

SELECT 'all/inner only: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.c = t2.c INNER JOIN t3 ON t1.c = t3.c ORDER BY ALL);

SELECT 'all/left only: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 LEFT JOIN t2 ON t1.c = t2.c LEFT JOIN t3 ON t1.c = t3.c ORDER BY ALL);

SELECT 'comma join rewritten to all/inner: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1, t2, t3 WHERE t1.c = t2.c AND t2.c = t3.c ORDER BY ALL);

SELECT 'single table: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (EXPLAIN SELECT * FROM t1 ORDER BY ALL);

-- A two-way ANY join is already rejected, because that join is the leftmost leaf's parent.
SELECT 'two-way inner/any: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 ANY INNER JOIN t2 ON 1 ORDER BY ALL);

-- Results: `ANY INNER JOIN ... ON 1` emits exactly one right-side match per left row.
SELECT 'inner/any non-leftmost: rows';
SELECT * FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL;

-- Only the row count is asserted here: with more than one left row and a constant join key, which
-- left row the ANY join keeps depends on the join order and is not part of the contract.
SELECT 'inner/any non-leftmost under a left leftmost join: row count';
SELECT count() FROM (SELECT * FROM t1 LEFT JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1);

SELECT 'all/inner only: rows';
SELECT * FROM t1 INNER JOIN t2 ON t1.c = t2.c INNER JOIN t3 ON t1.c = t3.c ORDER BY ALL;

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
