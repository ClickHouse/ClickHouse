-- The plan-based implementation distributes n-way joins that the query-based one has to refuse.
-- Query-based ships the whole join tree to every replica, so a non-leftmost join outside the set of
-- replica-safe shapes leaves no leaf coordinated and every replica evaluates the whole join, which
-- multiplies the rows - see `04651_pr_nway_join_non_leftmost_strictness`, which pins that
-- restriction for the query-based implementation. Plan-based places the split itself, so several of
-- the same shapes are distributed and still answer correctly; the rows are asserted below so that the
-- step count is not the only thing keeping this honest. The shapes that are not distributive over a
-- partition of the left side - a non-leftmost `FULL`, `ASOF`, `PASTE`, or `ANY INNER` on a constant -
-- must stay local in plan-based as well, and the second half asserts that.

DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;
DROP TABLE IF EXISTS t4 SYNC;

CREATE TABLE t1 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t1', 'r1') ORDER BY c;
CREATE TABLE t2 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t2', 'r1') ORDER BY c;
CREATE TABLE t3 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t3', 'r1') ORDER BY c;
-- `t4` carries the ASOF keys: they overlap `t1`, and each key has two candidates, so the ASOF answer
-- names one of them and the row assertion below is not vacuous.
CREATE TABLE t4 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t4', 'r1') ORDER BY c;

INSERT INTO t1 VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-02 00:00:00');
INSERT INTO t2 VALUES (2), (3);
INSERT INTO t3 VALUES (7, '2020-01-01 00:00:00'), (8, '2020-01-02 00:00:00');
INSERT INTO t4 VALUES (1, '2019-12-31 00:00:00'), (1, '2019-12-31 12:00:00'),
                      (2, '2020-01-01 00:00:00'), (2, '2020-01-01 12:00:00');

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1;
-- The step names below are the legacy ones; 'pretty' rewrites them.
SET explain_query_plan_default = 'legacy';
-- Stress threads inject `join_algorithm` and `join_use_nulls` as client options, which would change
-- the join results asserted here.
SET join_algorithm = 'hash', join_use_nulls = 0;

-- `ReadFromParallelReplicas` is the plan-based step; matching on the common suffix keeps the check
-- readable next to the query-based `ReadFromRemoteParallelReplicas`.

SELECT 'array join then any/right: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANY RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);
SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANY RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL;

SELECT 'array join then semi/right: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a SEMI RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);
SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a SEMI RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL;

SELECT 'array join then anti/right: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANTI RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL);
SELECT t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANTI RIGHT JOIN t2 ON t1.c = t2.c ORDER BY ALL;

-- A replica-safe shape, distributed by both implementations.
SELECT 'array join then all/inner: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a INNER JOIN t2 ON t1.c = t2.c ORDER BY ALL);
SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a INNER JOIN t2 ON t1.c = t2.c ORDER BY ALL;

-- A shape neither implementation distributes: the join is on a constant, so there is nothing to
-- coordinate by.
SELECT 'array join then inner/any: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a ANY INNER JOIN t3 ON 1 ORDER BY ALL);
SELECT t1.c, a FROM t1 ARRAY JOIN [1, 2] AS a ANY INNER JOIN t3 ON 1 ORDER BY ALL;

-- `RightAny`, which `any_join_distinct_right_table_keys = 1` makes of `ANY RIGHT`: only a read is
-- distributed, and the join stays on the initiator.
SELECT 'array join then right-any (legacy any): reads from replicas, join not shipped';
SELECT countIf(explain ILIKE '%ParallelReplicas%'), countIf(explain ILIKE '%JoinLogical%') FROM (
    EXPLAIN SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANY RIGHT JOIN t2 ON t1.c = t2.c
        ORDER BY ALL SETTINGS any_join_distinct_right_table_keys = 1);
SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1, 2] AS a ANY RIGHT JOIN t2 ON t1.c = t2.c
    ORDER BY ALL SETTINGS any_join_distinct_right_table_keys = 1;

-- A non-leftmost `LEFT` join is decided per left row whatever its strictness, so the join is shipped.
-- How many `JoinLogical` steps the fragment holds depends on the join reordering, which the test
-- runner randomizes, so only their presence is asserted.
SELECT 'left/any non-leftmost: reads from replicas, join shipped';
SELECT countIf(explain ILIKE '%ParallelReplicas%'), countIf(explain ILIKE '%JoinLogical%') > 0 FROM (
    EXPLAIN SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY LEFT JOIN t3 ON 1 ORDER BY ALL);
-- Which `t3` row the ANY join keeps for the constant key is not part of the contract.
SELECT t1.c, t2.c FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY LEFT JOIN t3 ON 1 ORDER BY ALL;

SELECT 'left/semi non-leftmost: reads from replicas, join shipped';
SELECT countIf(explain ILIKE '%ParallelReplicas%'), countIf(explain ILIKE '%JoinLogical%') > 0 FROM (
    EXPLAIN SELECT t1.c FROM t1 INNER JOIN t2 ON t1.c = t2.c SEMI LEFT JOIN t3 ON 1 ORDER BY ALL);
SELECT t1.c FROM t1 INNER JOIN t2 ON t1.c = t2.c SEMI LEFT JOIN t3 ON 1 ORDER BY ALL;

-- Shapes that are not distributive over a partition of the left side: applied on each replica and
-- concatenated, they would emit a row per replica, so nothing is read from the replicas.

SELECT 'inner/any non-leftmost: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL);
SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1 ORDER BY ALL;

-- Only the row count is asserted: with more than one left row and a constant join key, which left row
-- the ANY join keeps is not part of the contract.
SELECT 'inner/any non-leftmost under a left leftmost join: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT * FROM t1 LEFT JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1);
SELECT count() FROM (SELECT * FROM t1 LEFT JOIN t2 ON t1.c = t2.c ANY INNER JOIN t3 ON 1);

-- `t2.c = 3` has no match in `t1`, so the FULL join must emit it exactly once.
SELECT 'array join then full: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1] AS a FULL JOIN t2 ON t1.c = t2.c ORDER BY ALL);
SELECT t1.c, a, t2.c FROM t1 ARRAY JOIN [1] AS a FULL JOIN t2 ON t1.c = t2.c ORDER BY ALL;

-- `t2` leaves `t1.c = 2` as the only left row, and its ASOF match is the later of the two `t4` candidates.
SELECT 'asof/inner non-leftmost: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, t4.d FROM t1 INNER JOIN t2 ON t1.c = t2.c
        ASOF INNER JOIN t4 ON t1.c = t4.c AND t1.d > t4.d ORDER BY ALL);
SELECT t1.c, t4.d FROM t1 INNER JOIN t2 ON t1.c = t2.c
    ASOF INNER JOIN t4 ON t1.c = t4.c AND t1.d > t4.d ORDER BY ALL;

-- `PASTE JOIN` pairs rows by position. Only the plan is asserted, as in the query-based test: which
-- rows are paired depends on the read order.
SELECT 'paste non-leftmost: reads from replicas';
SELECT countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT t1.c, t2.c, t3.c FROM t1 INNER JOIN t2 ON t1.c = t2.c PASTE JOIN t3 ORDER BY ALL);

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
DROP TABLE t4 SYNC;
