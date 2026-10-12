-- A condition pushed into the fragment is spliced into the query the replicas run, and only a
-- condition they have too may order the initiator's copy of that fragment. The rewrite that does the
-- splicing refuses some fragments, so what matters here is that a refusal costs the ordering and
-- nothing else, and that a shape the rewrite does take keeps it.
--
--   * `LIMIT BY` is a subquery clause `rewriteSubquery` refuses, while the condition on the `LIMIT BY`
--     key is pushed below it and reaches the read: it prunes, and orders nothing;
--   * a non-deterministic `SELECT` list is refused by `hasNonRewritableFunction`, because the pushed
--     copy of the condition would call the function a second time - again the condition reaches the
--     read and must not order it;
--   * `untuple` is expanded to `tupleElement` before the query is shipped, so the rewrite takes it
--     like any other, the replicas do fix `tenant`, and the read may order itself off it.
--
-- The last is why this gate cannot simply withhold the ordering whenever it is unsure: the replicas
-- would read that fragment `InOrder` while the initiator read it `Default`, which is the same
-- disagreement from the other side.
--
-- The refusals not pinned here answer the same way for the same reason, and the shapes they name leave
-- nothing to withhold in the first place: a window function or a stateful function in the shipped
-- `SELECT` list is one the condition cannot be pushed below, a `LIMIT`, an `OFFSET` or a
-- `LIMIT AFTER .. UNTIL` keeps it from reaching the read, a `WITH` list is inlined by the analyzer and
-- a named subquery rewritten into the `FROM` before either can reach the rewrite, and a `FINAL` query
-- is not run by parallel replicas at all.

DROP TABLE IF EXISTS t_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_limit_by_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_nondet_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_untuple_pr_rewrite_shapes;

CREATE TABLE t_pr_rewrite_shapes (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_rewrite_shapes SELECT number % 10, number FROM numbers(10000);

CREATE VIEW v_limit_by_pr_rewrite_shapes AS
    SELECT tenant, ts FROM t_pr_rewrite_shapes ORDER BY ts LIMIT 1 BY tenant;
CREATE VIEW v_nondet_pr_rewrite_shapes AS
    SELECT tenant, ts, rand() AS r FROM t_pr_rewrite_shapes ORDER BY ts;
CREATE VIEW v_untuple_pr_rewrite_shapes AS
    SELECT tenant, ts, untuple((ts, ts + 1)) FROM t_pr_rewrite_shapes ORDER BY ts;

-- For runs with the old analyzer
SET enable_analyzer = 1;
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
-- These two decide whether the rewrite that carries the condition runs.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;

SELECT 'limit by: the condition reaches the read and prunes it, and orders nothing';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_limit_by_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts FROM v_limit_by_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'non-deterministic select list: the replicas do not get the condition, so nothing is ordered';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts, r FROM v_nondet_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts, r FROM v_nondet_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'untuple: the rewrite takes it, so the read may order itself off the condition';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT * FROM v_untuple_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT * FROM v_untuple_pr_rewrite_shapes WHERE tenant = 5);

DROP VIEW v_untuple_pr_rewrite_shapes;
DROP VIEW v_nondet_pr_rewrite_shapes;
DROP VIEW v_limit_by_pr_rewrite_shapes;
DROP TABLE t_pr_rewrite_shapes;
