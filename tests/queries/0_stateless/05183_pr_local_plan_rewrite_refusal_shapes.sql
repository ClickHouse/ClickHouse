-- The rewrite that splices a pushed condition into the query the replicas run refuses some fragments as
-- a whole, and then the condition stays above the initiator's copy of the fragment as well: what the
-- replicas do not get must not order that copy's read. One case per kind of refusal, and one shape that
-- looks refusable but is taken:
--
--   * a join tree of more than one table, which leaves the rewrite no single table to put the predicate
--     against (`findRewriteTarget`) - even with read-in-order asked to reach through the join;
--   * `LIMIT BY`, one of the subquery clauses `subqueryAcceptsPushedPredicate` refuses;
--   * a non-deterministic `SELECT` list, refused because the pushed copy of the condition would call the
--     function a second time;
--   * `untuple`, expanded to `tupleElement` before the query is shipped, so the rewrite takes it, the
--     replicas fix `tenant`, and the initiator's read may order itself off it too.
--
-- The other refusals answer the same way for the same reason: a window or stateful function in the
-- shipped `SELECT` list, a `LIMIT`, an `OFFSET`, a `LIMIT AFTER .. UNTIL`, a `UNION ALL` (05161). A `WITH`
-- list is inlined by the analyzer and a named subquery rewritten into the `FROM` before either reaches the
-- rewrite, and a `FINAL` query is not run by parallel replicas at all.

DROP TABLE IF EXISTS t_pr_rewrite_shapes;
DROP TABLE IF EXISTS d_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_join_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_limit_by_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_nondet_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_untuple_pr_rewrite_shapes;

CREATE TABLE t_pr_rewrite_shapes (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_rewrite_shapes SELECT number % 10, number FROM numbers(10000);

CREATE TABLE d_pr_rewrite_shapes (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO d_pr_rewrite_shapes SELECT number FROM numbers(10);

CREATE VIEW v_join_pr_rewrite_shapes AS
    SELECT t.tenant AS tenant, t.ts AS ts
    FROM t_pr_rewrite_shapes AS t INNER JOIN d_pr_rewrite_shapes AS d ON t.tenant = d.a
    ORDER BY t.ts;
CREATE VIEW v_limit_by_pr_rewrite_shapes AS
    SELECT tenant, ts FROM t_pr_rewrite_shapes ORDER BY ts LIMIT 1 BY tenant;
CREATE VIEW v_nondet_pr_rewrite_shapes AS
    SELECT tenant, ts, rand() AS r FROM t_pr_rewrite_shapes ORDER BY ts;
CREATE VIEW v_untuple_pr_rewrite_shapes AS
    SELECT tenant, ts, untuple((ts, ts + 1)) FROM t_pr_rewrite_shapes ORDER BY ts;

-- For runs with the old analyzer
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_allow_view_over_mergetree = 0;
SET parallel_replicas_plan_based = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_read_in_order = 1;
-- Ask for the ordering to reach through the join, and keep the join one that could carry it: the
-- spilling wrapper answers `hasDelayedBlocks`, which stops the descent before it starts.
SET query_plan_read_in_order_through_join = 1;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = false;
-- These two decide whether the rewrite that carries the condition runs.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;

SELECT 'join: the condition stays above the fragment, and nothing is ordered';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_join_pr_rewrite_shapes WHERE tenant = 2 LIMIT 5)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM v_join_pr_rewrite_shapes WHERE tenant = 2;

SELECT 'limit by: the same';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_limit_by_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts FROM v_limit_by_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'non-deterministic select list: the same';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts, r FROM v_nondet_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts, r FROM v_nondet_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'untuple: the rewrite takes it, so the condition goes in and orders the read';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_untuple_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT * FROM v_untuple_pr_rewrite_shapes WHERE tenant = 5);

DROP VIEW v_untuple_pr_rewrite_shapes;
DROP VIEW v_nondet_pr_rewrite_shapes;
DROP VIEW v_limit_by_pr_rewrite_shapes;
DROP VIEW v_join_pr_rewrite_shapes;
DROP TABLE d_pr_rewrite_shapes;
DROP TABLE t_pr_rewrite_shapes;
