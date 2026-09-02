-- A predicate that reaches the parallel-replicas local fragment without reaching the replicas must not
-- change the coordination mode the initiator announces. An equality on a sort key prefix is what could:
-- it fixes that column, which lets a sort or an aggregation on the rest of the key read in order, and
-- the initiator would then announce `WithOrder` against the replicas' `Default`.
--
-- Two things keep that from happening. Above the union, read-in-order is refused outright. Inside the
-- fragment - where a view's own `ORDER BY` puts it, and the refusal does not apply - an ordinary
-- predicate never gets in without `parallel_replicas_filter_pushdown`, which puts it in the replicas'
-- query as well. A join runtime filter does get in, and fixes nothing, so the mode is unchanged.

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

DROP TABLE b_pr_read_mode;
DROP VIEW v_sorted_pr_read_mode;
DROP VIEW v_pr_read_mode;
DROP TABLE t_pr_read_mode;
