-- `ReadFromRemote::addFilters` splices the condition into an AST whose join tree has to hold a single
-- table expression, so a fragment that joins keeps the query the replicas were given whatever
-- `parallel_replicas_filter_pushdown` asks for. The condition still enters the initiator's copy and
-- prunes it, and the read must stay unordered: ordering it off a condition only this replica has is
-- what makes the initiator announce `WithOrder` against the replicas' `Default`.
--
-- Read-in-order does reach through an inner join under `query_plan_read_in_order_through_join`, but
-- only for a sorting key prefix - which the replicas derive from their copy of the same fragment - and
-- not for a column a filter fixes. If that ever changes, the read below turns `InOrder` while the
-- replicas stay `Default`, and this test says so.

DROP TABLE IF EXISTS t_pr_join_fragment;
DROP TABLE IF EXISTS d_pr_join_fragment;
DROP VIEW IF EXISTS v_pr_join_fragment;

CREATE TABLE t_pr_join_fragment (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_join_fragment SELECT number % 4, number FROM numbers(10000);

CREATE TABLE d_pr_join_fragment (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO d_pr_join_fragment SELECT number FROM numbers(10);

-- The sort inside the view is what an equality on the sort key prefix could answer by reading in order.
CREATE VIEW v_pr_join_fragment AS
    SELECT t.tenant AS tenant, t.ts AS ts
    FROM t_pr_join_fragment AS t INNER JOIN d_pr_join_fragment AS d ON t.tenant = d.a
    ORDER BY t.ts;

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
-- Ask for the ordering to reach through the join, and keep the join one that could carry it: the
-- spilling wrapper answers `hasDelayedBlocks`, which stops the descent before it starts.
SET query_plan_read_in_order_through_join = 1;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = false;
-- The setting asks for the condition to be shipped; the join tree is what refuses it.
SET parallel_replicas_filter_pushdown = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;

SELECT 'the condition prunes the local read of a joining fragment without ordering it';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_pr_join_fragment WHERE tenant = 2 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';

SELECT 'and answers correctly';
SELECT count() FROM v_pr_join_fragment WHERE tenant = 2;
SELECT ts FROM v_pr_join_fragment WHERE tenant = 2 ORDER BY ts LIMIT 5;

DROP VIEW v_pr_join_fragment;
DROP TABLE d_pr_join_fragment;
DROP TABLE t_pr_join_fragment;
