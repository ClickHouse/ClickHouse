-- A fragment can hold more than one coordinated read: `parallel_replicas_allow_view_over_mergetree`
-- ships the outer query of a view, and a view that expands to `UNION ALL` puts every branch's read in
-- the one fragment, each answering to the coordinator. A condition arriving from outside that fragment
-- is pushed into it whole, and the branches must not derive ordering from it - the replicas execute the
-- same fragment without it.
--
-- The branches name their columns alike, which is the point: holding them to one set of column names
-- taken at the fragment root would let a column fixed in the first branch pass for the same-named
-- column in the second, and the second would read in order off a condition only this replica has.

DROP TABLE IF EXISTS t_pr_union_mode_a;
DROP TABLE IF EXISTS t_pr_union_mode_b;
DROP VIEW IF EXISTS v_pr_union_mode;
DROP VIEW IF EXISTS v_pr_union_mode_ordered;

CREATE TABLE t_pr_union_mode_a (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
CREATE TABLE t_pr_union_mode_b (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_union_mode_a SELECT number % 100, number FROM numbers(10000);
INSERT INTO t_pr_union_mode_b SELECT number % 100, number + 100000 FROM numbers(10000);

-- The first branch fixes `tenant` with a filter of its own; the second fixes nothing. Both branches
-- travel to the replicas, so the first branch's ordering is one the replicas derive as well.
CREATE VIEW v_pr_union_mode AS
    SELECT tenant, ts FROM t_pr_union_mode_a WHERE tenant = 5
    UNION ALL
    SELECT tenant, ts FROM t_pr_union_mode_b;
-- The sort has to be inside the fragment for either branch to read in order.
CREATE VIEW v_pr_union_mode_ordered AS SELECT tenant, ts FROM v_pr_union_mode ORDER BY ts;

-- For runs with the old analyzer
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_plan_based = 0;
-- What puts both branches' reads in one fragment.
SET parallel_replicas_allow_view_over_mergetree = 1;
-- The condition must stay out of the replicas' query, which is what this setting would put it in.
SET parallel_replicas_filter_pushdown = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_read_in_order = 1;

-- Without this the remote replicas may get no marks at all, and then they never send a read request
-- for the coordinator to check the mode of.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

SELECT 'the fragment reads both branches, only the one with its own filter in order';
-- `InOrder` for the branch that fixes `tenant` itself, `Default` for the branch that does not: the
-- pushed `tenant = 5` prunes both and orders neither.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT * FROM v_pr_union_mode_ordered WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Read type%';

SELECT 'and answers correctly';
SELECT count() FROM v_pr_union_mode_ordered WHERE tenant = 5;
SELECT * FROM v_pr_union_mode_ordered WHERE tenant = 5 ORDER BY ts LIMIT 5;

SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

DROP VIEW v_pr_union_mode_ordered;
DROP VIEW v_pr_union_mode;
DROP TABLE t_pr_union_mode_b;
DROP TABLE t_pr_union_mode_a;
