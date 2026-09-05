-- A predicate that reaches the parallel-replicas local fragment without reaching the replicas must not
-- change the coordination mode the initiator announces. An equality on a sort key prefix is what could:
-- it fixes that column, which lets a sort or an aggregation on the rest of the key read in order, and
-- the initiator would then announce `WithOrder` against the replicas' `Default`.
--
-- Two things keep that from happening. Above the union, read-in-order is refused outright. Inside the
-- fragment - where a view's own `ORDER BY` puts it, and the refusal does not apply - a predicate that
-- contains an equality anywhere never gets in without `parallel_replicas_filter_pushdown`, which puts
-- it in the replicas' query as well. That is a coarse test, refusing far more than the equalities on a
-- sort key prefix it is aimed at, but a join runtime filter carries no equality: it does get in, and
-- fixes nothing, so the mode is unchanged.
--
-- `parallel_replicas_filter_pushdown` ships the predicate by rewriting the replicas' query, so it is
-- only as good as that rewrite. The last section takes away, one at a time, each thing the rewrite
-- needs: the two settings, then a query it can attribute the predicate to, then a query it will rewrite
-- at all. In each case the predicate has to stay out of the local plan again, setting or no setting.

DROP TABLE IF EXISTS t_pr_read_mode;
DROP VIEW IF EXISTS v_pr_read_mode;

CREATE TABLE t_pr_read_mode (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts) SETTINGS index_granularity = 128;
-- Read through a view, so that the filter starts above the part parallel replicas execute.
CREATE VIEW v_pr_read_mode AS SELECT * FROM t_pr_read_mode;
INSERT INTO t_pr_read_mode SELECT number % 100, number FROM numbers(10000);

-- For runs with the old analyzer
SET enable_analyzer = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;

SELECT 'single node';
SET optimize_read_in_order = 1, optimize_aggregation_in_order = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts FROM v_pr_read_mode WHERE tenant = 42 ORDER BY ts LIMIT 5)
WHERE explain LIKE '%Read type%';
SET optimize_read_in_order = 0, optimize_aggregation_in_order = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts, count() FROM v_pr_read_mode WHERE tenant = 42 GROUP BY ts)
WHERE explain LIKE '%Read type%';

SELECT 'parallel replicas';
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_allow_view_over_mergetree = 0;
SET parallel_replicas_plan_based = 0;
-- The condition is shipped to the replicas by rewriting their query, so pin the settings that decide
-- whether that rewrite happens and reaches them - the assertions below are about which side has the
-- condition. The end of the test turns each of them off in turn.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
-- `parallel_replicas_filter_pushdown` is left at its default, so the replicas never see the filter.

SET optimize_read_in_order = 1, optimize_aggregation_in_order = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts FROM v_pr_read_mode WHERE tenant = 42 ORDER BY ts LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT ts FROM v_pr_read_mode WHERE tenant = 42 ORDER BY ts LIMIT 5;

SET optimize_read_in_order = 0, optimize_aggregation_in_order = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts, count() FROM v_pr_read_mode WHERE tenant = 42 GROUP BY ts)
WHERE explain LIKE '%Read type%';
SELECT count(), sum(ts) FROM (SELECT ts, count() FROM v_pr_read_mode WHERE tenant = 42 GROUP BY ts);

SELECT 'sort inside the fragment';
-- The view sorts, so read-in-order is decided inside the fragment, where the union refusal does not
-- apply. The predicate must stay out of the local plan unless the replicas get it too.
DROP VIEW IF EXISTS v_sorted_pr_read_mode;
CREATE VIEW v_sorted_pr_read_mode AS SELECT * FROM t_pr_read_mode ORDER BY ts;

SET optimize_read_in_order = 1, optimize_aggregation_in_order = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5;

SELECT 'sort inside the fragment, filter shipped to the replicas too';
SET parallel_replicas_filter_pushdown = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5;
SET parallel_replicas_filter_pushdown = 0;

SELECT 'sort inside the fragment, join runtime filter';
-- The runtime filter does reach the local plan here, and must leave the mode alone.
DROP TABLE IF EXISTS b_pr_read_mode;
CREATE TABLE b_pr_read_mode (tenant UInt64) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO b_pr_read_mode VALUES (42);
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = false;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT v.ts FROM v_sorted_pr_read_mode AS v JOIN b_pr_read_mode AS bb ON v.tenant = bb.tenant
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Runtime filters:%';
SELECT count() FROM (SELECT v.ts FROM v_sorted_pr_read_mode AS v JOIN b_pr_read_mode AS bb ON v.tenant = bb.tenant);

SELECT 'sort inside the fragment, runtime filter conjoined with an ordinary one';
-- The two conditions get merged into one `Filter`. Only the runtime filter half may go into the local
-- plan; `tenant = 42` has to stay above it, or the read would go in order and the replicas would not.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT v.ts FROM v_sorted_pr_read_mode AS v JOIN b_pr_read_mode AS bb ON v.tenant = bb.tenant
    WHERE v.tenant = 42 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Runtime filters:%' OR explain LIKE '%Prewhere filter column%';
SELECT v.ts FROM v_sorted_pr_read_mode AS v JOIN b_pr_read_mode AS bb ON v.tenant = bb.tenant
WHERE v.tenant = 42 ORDER BY v.ts LIMIT 5;

SELECT 'sort inside the fragment, the filter cannot reach the replicas';
-- `parallel_replicas_filter_pushdown` ships the condition by rewriting the replicas' query. Where that
-- rewrite cannot happen the condition stays on the initiator, so it must not go into the local plan
-- either - otherwise the initiator reads in order and announces `WithOrder` while the replicas, still
-- reading their own unfiltered query, announce `Default`.
SET enable_join_runtime_filters = 0;
SET parallel_replicas_filter_pushdown = 1;

SELECT 'no AST predicate push-down';
SET allow_push_predicate_ast_for_distributed_subqueries = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT 'the replicas run a serialized plan';
SET serialize_query_plan = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT ts FROM v_sorted_pr_read_mode WHERE tenant = 42 LIMIT 5;
SET serialize_query_plan = 0;

SELECT 'the shipped query reads two tables';
-- The join is inside the fragment here, not above it, so the query shipped to the replicas is the join.
-- `addFilters` rewrites a shipped query by attributing the predicate to the single table it reads, and
-- with two of them it does nothing at all - the setting is on, and the replicas still never see
-- `tenant = 42`. The `Filter` has to stay above the fragment's own `Sorting`; were it pushed below, it
-- would reach the read as a `Prewhere` and the initiator alone would be filtering.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT ts FROM (
        SELECT t.ts AS ts, t.tenant AS tenant
        FROM t_pr_read_mode AS t JOIN b_pr_read_mode AS bb ON t.tenant = bb.tenant
        ORDER BY t.ts
    ) WHERE tenant = 42 LIMIT 5
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Filter column%' OR explain LIKE '%Prewhere filter column%'
   OR explain LIKE '%Read type%';
SELECT ts FROM (
    SELECT t.ts AS ts, t.tenant AS tenant
    FROM t_pr_read_mode AS t JOIN b_pr_read_mode AS bb ON t.tenant = bb.tenant
    ORDER BY t.ts
) WHERE tenant = 42 LIMIT 5;

SELECT 'the shipped query selects a window function';
-- The shape is fine here - one table - but `PredicateRewriteVisitor` refuses to rewrite a subquery whose
-- `SELECT` list holds a window function, and it refuses for every predicate at once, this one included.
-- So again the setting is on and the replicas do not get `tenant = 42`.
-- `c` has to be selected by the outer query, or it is pruned away and the shipped query carries no
-- window function at all.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT ts, c FROM (
        SELECT tenant, ts, count() OVER (PARTITION BY tenant) AS c FROM t_pr_read_mode ORDER BY ts
    ) WHERE tenant = 42 LIMIT 5
)
WHERE explain LIKE '%Sorting%' OR explain LIKE '%Filter column%' OR explain LIKE '%Prewhere filter column%'
   OR explain LIKE '%Read type%';
SELECT ts, c FROM (
    SELECT tenant, ts, count() OVER (PARTITION BY tenant) AS c FROM t_pr_read_mode ORDER BY ts
) WHERE tenant = 42 LIMIT 5;

SELECT 'the shipped query carries its own SETTINGS';
-- A jointly scoped `SETTINGS` clause gives the shipped query its own context, and its value is the one
-- that governs what the replicas run - see 04746_distributed_plan_execute_locally_subquery_settings. So
-- the decision has to be read from there and not from the ambient query, in both directions.
SET parallel_replicas_filter_pushdown = 0;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT 'off outside, on inside: the replicas do get it';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT ts FROM (SELECT * FROM t_pr_read_mode ORDER BY ts SETTINGS parallel_replicas_filter_pushdown = 1)
    WHERE tenant = 42 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT ts FROM (SELECT * FROM t_pr_read_mode ORDER BY ts SETTINGS parallel_replicas_filter_pushdown = 1)
WHERE tenant = 42 LIMIT 5;

SELECT 'on outside, off inside: they do not';
SET parallel_replicas_filter_pushdown = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT ts FROM (SELECT * FROM t_pr_read_mode ORDER BY ts SETTINGS parallel_replicas_filter_pushdown = 0)
    WHERE tenant = 42 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT ts FROM (SELECT * FROM t_pr_read_mode ORDER BY ts SETTINGS parallel_replicas_filter_pushdown = 0)
WHERE tenant = 42 LIMIT 5;

SELECT 'on outside, AST push-down off inside: they do not either';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT ts FROM (
        SELECT * FROM t_pr_read_mode ORDER BY ts
        SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0
    ) WHERE tenant = 42 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT ts FROM (
    SELECT * FROM t_pr_read_mode ORDER BY ts
    SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0
) WHERE tenant = 42 LIMIT 5;

DROP TABLE b_pr_read_mode;
DROP VIEW v_sorted_pr_read_mode;
DROP VIEW v_pr_read_mode;
DROP TABLE t_pr_read_mode;
