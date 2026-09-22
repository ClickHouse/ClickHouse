-- A JOIN whose broadcast side is a derived table with `IN (subquery)` must run under plan-based
-- parallel replicas instead of failing to clone a `DelayedCreatingSetsStep` holding sets.
-- https://github.com/ClickHouse/ClickHouse/issues/120451

DROP TABLE IF EXISTS t_pr_join_fact;
DROP TABLE IF EXISTS t_pr_join_dim;
DROP TABLE IF EXISTS t_pr_join_allow;

CREATE TABLE t_pr_join_fact (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pr_join_dim (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pr_join_allow (k Int64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_pr_join_fact SELECT number FROM numbers(20);
INSERT INTO t_pr_join_dim SELECT number FROM numbers(15);
INSERT INTO t_pr_join_allow SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;

SELECT count() FROM t_pr_join_fact ALL LEFT JOIN
    (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
    ON t_pr_join_fact.k = r.k;

SELECT countIf(r.k > 0) FROM t_pr_join_fact ALL LEFT JOIN
    (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
    ON t_pr_join_fact.k = r.k;

SELECT count() FROM t_pr_join_fact ALL INNER JOIN
    (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
    ON t_pr_join_fact.k = r.k;

SELECT count() FROM t_pr_join_fact ALL INNER JOIN
    (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
    ON t_pr_join_fact.k = r.k
SETTINGS query_plan_optimize_join_order_limit = 0;

-- The read of the left side still goes to the replicas, while the join stays on the initiator: the `Join`
-- step is above the `Union` of the local read and `ReadFromParallelReplicas` instead of below it.
SELECT
    arrayExists(x -> x LIKE '%ReadFromParallelReplicas%', plan) AS read_distributed,
    arrayFirstIndex(x -> x LIKE '%──Join%', plan) < arrayFirstIndex(x -> x LIKE '%──Union%', plan) AS join_local
FROM
(
    SELECT groupArray(explain) AS plan
    FROM (EXPLAIN optimize = 1, description = 0
        SELECT count() FROM t_pr_join_fact ALL LEFT JOIN
            (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
            ON t_pr_join_fact.k = r.k)
);

-- With the sides swapped, the distributed read is the build side of the join that stays on the initiator.
-- The runtime filter must be built on the initiator from all rows instead of on each replica from its own
-- share of them, otherwise the probe side is pruned by a partial filter.
SET query_plan_join_swap_table = 'true';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_runtime_filter_min_probe_rows = 0;

SELECT countIf(r.k > 0) FROM t_pr_join_fact ALL LEFT JOIN
    (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
    ON t_pr_join_fact.k = r.k;

SELECT
    arrayExists(x -> x LIKE '%ReadFromParallelReplicas%', plan) AS read_distributed,
    arrayFirstIndex(x -> x LIKE '%──BuildRuntimeFilter%', plan) < arrayFirstIndex(x -> x LIKE '%──Union%', plan) AS filter_local
FROM
(
    SELECT groupArray(explain) AS plan
    FROM (EXPLAIN optimize = 1, description = 0
        SELECT countIf(r.k > 0) FROM t_pr_join_fact ALL LEFT JOIN
            (SELECT k FROM t_pr_join_dim WHERE k IN (SELECT k FROM t_pr_join_allow)) AS r
            ON t_pr_join_fact.k = r.k)
);

DROP TABLE t_pr_join_fact;
DROP TABLE t_pr_join_dim;
DROP TABLE t_pr_join_allow;
