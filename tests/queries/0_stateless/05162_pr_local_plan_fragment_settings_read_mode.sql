-- The condition pushed into the initiator's local fragment is spliced into the query the replicas run,
-- and the initiator may read in order off it only where that splice happens. Two settings decide
-- whether it does:
--
--   * `allow_push_predicate_ast_for_distributed_subqueries` - `addFilters` returns without it;
--   * `serialize_query_plan` - the splice is written into an AST, which is not what the replicas
--     execute when the plan is shipped instead.
--
-- Both are read from the settings the fragment carries, not from the outer query, because the fragment
-- is what travels and the rewrite answers to those. Read them off the outer query and the initiator
-- fixes `tenant` while the replicas never see the condition: it reads in order against their `Default`
-- and the query fails with "Got read request from replica N for unknown stream".

DROP TABLE IF EXISTS t_pr_fragment_settings;

CREATE TABLE t_pr_fragment_settings (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_fragment_settings SELECT number % 100, number FROM numbers(10000);

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
-- The sort inside the subquery is what an equality on the sort key prefix could answer by reading in
-- order, so ask for one.
SET optimize_read_in_order = 1;
-- Pin both, so only the one under test decides.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;

SELECT 'the fragment carries the setting, so the answer is read from there, not from the outer query';
-- The session says the rewrite may run; the subquery that travels says it may not, and the subquery is
-- what the rewrite answers to. Read the outer `1` instead and the initiator fixes `tenant` while the
-- replicas never see the condition.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0
    ) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0
    ) WHERE tenant = 5 ORDER BY ts LIMIT 95, 5
);

SELECT 'and the other way round: the session says no, the fragment says yes, and the read is ordered';
SET allow_push_predicate_ast_for_distributed_subqueries = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1
    ) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1
    ) WHERE tenant = 5 ORDER BY ts LIMIT 95, 5
);
SET allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT 'and the replicas must be running that AST rather than a shipped plan';
SET serialize_query_plan = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts)
    WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (
    SELECT tenant, ts FROM (SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts)
    WHERE tenant = 5 ORDER BY ts LIMIT 95, 5
);
SET serialize_query_plan = 0;

DROP TABLE t_pr_fragment_settings;
