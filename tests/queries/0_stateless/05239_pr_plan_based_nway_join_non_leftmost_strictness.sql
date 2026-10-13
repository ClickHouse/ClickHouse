-- The plan-based implementation distributes n-way joins that the query-based one has to refuse.
-- Query-based ships the whole join tree to every replica, so a non-leftmost join outside the set of
-- replica-safe shapes leaves no leaf coordinated and every replica evaluates the whole join, which
-- multiplies the rows - see `04651_pr_nway_join_non_leftmost_strictness`, which pins that
-- restriction for the query-based implementation. Plan-based places the split itself, so the same
-- shapes are distributed and still answer correctly; the rows are asserted below so that the step
-- count is not the only thing keeping this honest.

DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t1', 'r1') ORDER BY c;
CREATE TABLE t2 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t2', 'r1') ORDER BY c;
CREATE TABLE t3 (c Int32, d DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t3', 'r1') ORDER BY c;

INSERT INTO t1 VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-02 00:00:00');
INSERT INTO t2 VALUES (2), (3);
INSERT INTO t3 VALUES (7, '2020-01-01 00:00:00'), (8, '2020-01-02 00:00:00');

SET enable_analyzer = 1;
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

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
