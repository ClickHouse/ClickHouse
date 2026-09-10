-- `parallel_replicas_filter_pushdown` decides whether the condition pushed into the initiator's local
-- fragment is also spliced into the query the replicas run. The fragment travels with its own
-- `SETTINGS`, and the rewrite answers to those - so a subquery that turns the setting off keeps the
-- condition local however the outer query is set.
--
-- Read that answer off the outer query instead and the initiator fixes `tenant` while the replicas
-- never see the condition: it reads in order against their `Default` and the query fails with
-- "Got read request from replica N for unknown stream".

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
-- Pin the two settings the rewrite into the replicas' query needs, so only the one under test decides.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
-- The outer query says the replicas get the condition; the fragment below says they do not.
SET parallel_replicas_filter_pushdown = 1;

SELECT 'the fragment keeps the condition, so it does not order the read';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS parallel_replicas_filter_pushdown = 0
    ) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';

SELECT 'and answers instead of failing on the coordination mode';
SELECT count() FROM (
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS parallel_replicas_filter_pushdown = 0
    ) WHERE tenant = 5 LIMIT 5
);

SELECT 'with the fragment set to ship it, the read is ordered as the replicas order theirs';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_fragment_settings ORDER BY ts
        SETTINGS parallel_replicas_filter_pushdown = 1
    ) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';

DROP TABLE t_pr_fragment_settings;
