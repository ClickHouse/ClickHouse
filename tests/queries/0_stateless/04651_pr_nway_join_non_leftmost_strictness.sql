-- Parallel replicas must be disabled for an n-way join whose non-leftmost join is not replica-safe.
-- The whole join tree is shipped to every replica, but a leaf's reads are coordinated only for the
-- join shapes the search for that leaf descends. A non-leftmost join outside that set leaves no leaf
-- coordinated, so every replica evaluates the whole join and the initiator concatenates the copies,
-- multiplying every row by the replica count.

DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;
DROP TABLE IF EXISTS t4 SYNC;

CREATE TABLE t1 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t1', 'r1') ORDER BY c;
CREATE TABLE t2 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t2', 'r1') ORDER BY c;
CREATE TABLE t3 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t3', 'r1') ORDER BY c;
-- `t4` carries the ASOF keys: they overlap `t1` (`t3`'s do not), and each key has two candidates, so
-- the ASOF answer names one of them and the row assertion below is not vacuous.
CREATE TABLE t4 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t4', 'r1') ORDER BY c;

INSERT INTO t1 VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-02 00:00:00');
INSERT INTO t2 VALUES (2), (3);
INSERT INTO t3 VALUES (7, '2020-01-01 00:00:00'), (8, '2020-01-02 00:00:00');
INSERT INTO t4 VALUES (1, '2019-12-31 00:00:00'), (1, '2019-12-31 12:00:00'),
                      (2, '2020-01-01 00:00:00'), (2, '2020-01-01 12:00:00');

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
-- The plan-shape assertions below grep the legacy EXPLAIN step names; 'pretty' rewrites them.
SET explain_query_plan_default = 'legacy';
-- Stress threads inject `join_algorithm` and `join_use_nulls` as client options, which would make the
-- ASOF assertion throw and would turn the FULL join's default-filled cells into NULL.
SET join_algorithm = 'hash', join_use_nulls = 0;

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

-- A non-leftmost FULL join is unsafe while carrying ALL strictness: it emits unmatched right rows,
-- which each replica would decide from its own slice of the left side. The pre-existing
-- FULL/GLOBAL/CROSS rule cannot catch this shape, because the ARRAY JOIN does not increment
-- `joins_count`, so the kind term of the new veto covers it.
SELECT 'array join then full: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a FULL JOIN t2 ON t1.c = t2.c ORDER BY ALL);

-- The RIGHT axis is decided by the strictness term alone: `ALL RIGHT` after an ARRAY JOIN stays
-- eligible (the residual described in the PR description, which keeps
-- 03452_array_join_global_right_join_parallel_replicas exercising that path), while every other
-- strictness reachable with kind RIGHT is vetoed: `ANY`, `SEMI`, `ANTI` and the `RightAny` that
-- `any_join_distinct_right_table_keys = 1` produces. Both directions are asserted, and each vetoed
-- strictness has its own line, so none of them can be dropped from the veto unnoticed.
SELECT 'array join then all/right: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);

SELECT 'array join then any/right: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANY RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);

SELECT 'array join then semi/right: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a SEMI RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);

-- `ANTI RIGHT` is vetoed by the same strictness term: the parser accepts `ANTI` with kind `RIGHT`
-- (it rejects `SEMI`/`ANTI` only for the other kinds), and `Anti` is not `ALL`.
SELECT 'array join then anti/right: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANTI RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);

-- Under `any_join_distinct_right_table_keys = 1` the query-tree builder converts an `ANY RIGHT`
-- join's strictness to `RightAny` (its rewrite to `SEMI LEFT` fires only for `INNER`), which is also
-- not `ALL` and so also vetoed. The setting is statement-level on purpose: as a file-level `SET` it
-- would change the strictness resolution of every later statement too.
SELECT 'array join then right-any (legacy any): reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANY RIGHT JOIN t2 ON t1.c = t2.c
        ORDER BY ALL SETTINGS any_join_distinct_right_table_keys = 1);

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

-- The LEFT exemption is load-bearing: the search for the coordinated leaf descends through `LEFT`
-- whatever the strictness, so the left side really is partitioned there and `ANY LEFT` / `SEMI LEFT`
-- stay eligible. Being per-left-row is not what earns the exemption (see the ASOF case below).
SELECT 'left/any non-leftmost: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY LEFT JOIN t3 ON 1 ORDER BY ALL);

SELECT 'left/semi non-leftmost: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c FROM t1 INNER JOIN t2 ON t1.c = t2.c SEMI LEFT JOIN t3 ON 1 ORDER BY ALL);

-- `ASOF INNER` decides each left row on its own, and is vetoed anyway: outside `LEFT` the search for
-- the coordinated leaf does not descend, so nothing partitions the left side and every replica
-- returns the whole join. Its row assertion below is the measurement, not an argument.
SELECT 'asof/inner non-leftmost: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, t4.d FROM t1 INNER JOIN t2 ON t1.c = t2.c
        ASOF INNER JOIN t4 ON t1.c = t4.c AND t1.d > t4.d ORDER BY ALL);

-- `PASTE JOIN` pairs rows by position, so it is not distributive over a partition of the left side
-- either. It carries `ALL` strictness (the parser forbids an explicit `ANY`/`ALL` on `PASTE`, and
-- `Unspecified` is normalized to `join_default_strictness`), so it needs a kind-level veto.
SELECT 'paste non-leftmost: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c PASTE JOIN t3 ORDER BY ALL);

-- A `PASTE JOIN` in the leftmost slot is already ineligible on `master`
-- (`allowParallelReplicasForJoinTree` admits only `(Inner, All)`, `Left` and a simple `RIGHT`),
-- so this is a guard rather than a discriminator.
SELECT 'two-way paste: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, t3.c FROM t1 PASTE JOIN t3 ORDER BY ALL);

-- Results: `ANY INNER JOIN ... ON 1` emits exactly one pair for the constant key --
-- `ConstantJoin` gives `INNER ANY` `left_rows_to_join = FirstRowOnly`, so the join
-- contributes a single row however many left rows there are.
SELECT 'inner/any non-leftmost: rows';
SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL;

-- Only the row count is asserted here: with more than one left row and a constant join key, which
-- left row the ANY join keeps depends on the join order and is not part of the contract.
SELECT 'inner/any non-leftmost under a left leftmost join: row count';
SELECT count() FROM (SELECT * FROM t1 LEFT JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1);

-- Positive control for the still-eligible (distributive) direction: the same non-leftmost
-- constant-key join with `ALL` strictness must emit every left x right pair exactly once.
-- Parallel replicas stay enabled for this shape, so a duplicated row here would mean the
-- new veto is too narrow.
SELECT 'all/inner non-leftmost on a constant: rows';
SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c INNER JOIN t3 ON 1 ORDER BY ALL;

-- Row control for the FULL half: `t2.c = 3` has no match in `t1`, so a FULL join must emit it
-- exactly once. Applied independently on each replica and concatenated it would appear per replica.
SELECT 'array join then full: rows';
SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1] AS a FULL JOIN t2 ON t1.c = t2.c ORDER BY ALL;

-- Row control for the ASOF half: `t2` leaves `t1.c = 2` as the only left row, and its ASOF match is
-- the later of the two `t4` candidates. Without the veto this line returns that row once per replica,
-- and it does so at `parallel_replicas_local_plan = 0`, which is why the value is pinned here.
SELECT 'asof/inner non-leftmost: rows';
SELECT t1.c, t4.d FROM t1 INNER JOIN t2 ON t1.c = t2.c
    ASOF INNER JOIN t4 ON t1.c = t4.c AND t1.d > t4.d ORDER BY ALL
    SETTINGS parallel_replicas_local_plan = 0;

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
DROP TABLE t4 SYNC;
