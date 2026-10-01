-- A condition pushed into the fragment is spliced into the query the replicas run, and only a
-- condition they have too may order the initiator's copy of that fragment.
-- The rewrite that does the splicing refuses some fragment shapes, so the two shapes named most often
-- are pinned here: neither lets the initiator order a read the replicas read unordered.
--
--   * a window function in the shipped `SELECT` list is a shape `rewriteSubquery` refuses - and the
--     fragment derives no ordering from the condition anyway, so there is nothing to withhold;
--   * `untuple` is expanded to `tupleElement` before the query is shipped, so the rewrite takes it
--     like any other, the replicas do fix `tenant`, and the read may order itself off it.
--
-- The second is why this gate cannot simply withhold ordering whenever it is unsure: the replicas
-- would read that fragment `InOrder` while the initiator read it `Default`, which is the same
-- disagreement from the other side.

DROP TABLE IF EXISTS t_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_window_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_untuple_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_limit_by_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_with_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_stateful_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_nondet_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_limit_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_offset_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_range_pr_rewrite_shapes;
DROP VIEW IF EXISTS v_cte_pr_rewrite_shapes;
DROP TABLE IF EXISTS r_pr_rewrite_shapes;

CREATE TABLE t_pr_rewrite_shapes (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_rewrite_shapes SELECT number % 10, number FROM numbers(10000);

-- `FINAL` is refused by the rewrite as well, and parallel replicas do not run such a query at all.
CREATE TABLE r_pr_rewrite_shapes (tenant UInt64, ts UInt64) ENGINE = ReplacingMergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO r_pr_rewrite_shapes SELECT number % 10, number FROM numbers(10000);

CREATE VIEW v_window_pr_rewrite_shapes AS
    SELECT tenant, ts, sum(ts) OVER (PARTITION BY tenant ORDER BY ts) AS s FROM t_pr_rewrite_shapes ORDER BY ts;
CREATE VIEW v_untuple_pr_rewrite_shapes AS
    SELECT tenant, ts, untuple((ts, ts + 1)) FROM t_pr_rewrite_shapes ORDER BY ts;
-- A condition on the `LIMIT BY` key is pushed below the `LIMIT BY` and reaches the read, where it
-- fixes the sort key prefix - but `rewriteSubquery` refuses a subquery that has one.
CREATE VIEW v_limit_by_pr_rewrite_shapes AS
    SELECT tenant, ts FROM t_pr_rewrite_shapes ORDER BY ts LIMIT 1 BY tenant;
-- A `WITH` list is a shape `rewriteSubquery` refuses when
-- `allow_push_predicate_when_subquery_contains_with` is off - but the analyzer inlines the alias and
-- evaluates the scalar subquery before the query is shipped, so no `WITH` ever reaches the rewrite.
CREATE VIEW v_with_pr_rewrite_shapes AS
    WITH 7 AS seven, (SELECT max(ts) FROM t_pr_rewrite_shapes) AS mx
    SELECT tenant, ts, seven, mx FROM t_pr_rewrite_shapes ORDER BY ts;
-- A stateful function in the `SELECT` list is refused as well, and leaves nothing to withhold: a
-- condition may not be pushed below one, because filtering first would change what the function
-- computes. Both sides key on the same `isStateful`, so this holds for every such function rather
-- than for the one picked here - including a deterministic one, which cannot be written here because
-- the ones that exist need credentials or a `TimeSeries` context to be planned at all.
CREATE VIEW v_stateful_pr_rewrite_shapes AS
    SELECT tenant, ts, rowNumberInAllBlocks() AS rn FROM t_pr_rewrite_shapes ORDER BY ts;
-- The control: non-deterministic but not stateful. A condition does go below this one, so the read
-- orders itself off it - which is what tells statefulness apart from non-determinism as the reason.
CREATE VIEW v_nondet_pr_rewrite_shapes AS
    SELECT tenant, ts, rand() AS r FROM t_pr_rewrite_shapes ORDER BY ts;
-- The rest of the list `rewriteSubquery` refuses on. A `LIMIT` of any kind keeps the condition from
-- reaching the read at all, so there is nothing left to order by the time it matters.
CREATE VIEW v_limit_pr_rewrite_shapes AS
    SELECT tenant, ts FROM t_pr_rewrite_shapes ORDER BY ts LIMIT 9999;
CREATE VIEW v_offset_pr_rewrite_shapes AS
    SELECT tenant, ts FROM t_pr_rewrite_shapes ORDER BY ts LIMIT 9000 OFFSET 10;
CREATE VIEW v_range_pr_rewrite_shapes AS
    SELECT tenant, ts FROM t_pr_rewrite_shapes ORDER BY ts LIMIT AFTER 5 UNTIL 9999;
-- A `WITH` that names a subquery is rewritten into a subquery in the `FROM`, so the shipped query
-- carries no `WITH` for the rewrite to refuse and the condition travels like any other.
CREATE VIEW v_cte_pr_rewrite_shapes AS
    WITH c AS (SELECT tenant, ts FROM t_pr_rewrite_shapes) SELECT tenant, ts FROM c ORDER BY ts;

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

SELECT 'window function in the shipped select list: nothing is ordered either way';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts, s FROM v_window_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (SELECT tenant, ts, s FROM v_window_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'untuple: the rewrite takes it, so the read may order itself off the condition';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT * FROM v_untuple_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT * FROM v_untuple_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'limit by: the condition reaches the read and prunes it, and orders nothing';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_limit_by_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts FROM v_limit_by_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'a with list never reaches the rewrite, so the read is ordered as the replicas order theirs';
SET allow_push_predicate_when_subquery_contains_with = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts, seven, mx FROM v_with_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (SELECT tenant, ts, seven, mx FROM v_with_pr_rewrite_shapes WHERE tenant = 5);
SET allow_push_predicate_when_subquery_contains_with = 1;

SELECT 'stateful select list: the condition cannot go below it, so nothing is ordered';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts, rn FROM v_stateful_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (SELECT tenant, ts, rn FROM v_stateful_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'non-deterministic but not stateful: the condition does go below it';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts, r FROM v_nondet_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts, r FROM v_nondet_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'every kind of limit keeps the condition away from the read';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_limit_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_offset_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_range_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%';

SELECT 'a named subquery is not a with list, so the condition travels and orders the read';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_cte_pr_rewrite_shapes WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%' OR explain LIKE '%Prewhere filter column%';
SELECT count() FROM (SELECT tenant, ts FROM v_cte_pr_rewrite_shapes WHERE tenant = 5);

SELECT 'final: parallel replicas do not run the query at all, so there is no mode to agree on';
SELECT count() AS shipped_fragments
FROM (EXPLAIN description = 0 SELECT tenant, ts FROM (SELECT tenant, ts FROM r_pr_rewrite_shapes FINAL ORDER BY ts) WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

DROP TABLE r_pr_rewrite_shapes;
DROP VIEW v_cte_pr_rewrite_shapes;
DROP VIEW v_range_pr_rewrite_shapes;
DROP VIEW v_offset_pr_rewrite_shapes;
DROP VIEW v_limit_pr_rewrite_shapes;
DROP VIEW v_nondet_pr_rewrite_shapes;
DROP VIEW v_stateful_pr_rewrite_shapes;
DROP VIEW v_with_pr_rewrite_shapes;
DROP VIEW v_limit_by_pr_rewrite_shapes;
DROP VIEW v_untuple_pr_rewrite_shapes;
DROP VIEW v_window_pr_rewrite_shapes;
DROP TABLE t_pr_rewrite_shapes;
