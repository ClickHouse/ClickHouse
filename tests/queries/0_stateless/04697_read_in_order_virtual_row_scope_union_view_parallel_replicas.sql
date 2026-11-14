-- Tags: no-random-settings, no-random-merge-tree-settings

-- `04671_read_in_order_virtual_row_scope_parallel_replicas_local_plan` pins that the initiator-local
-- fragment of a parallel-replicas read is built under the settings the fragment is shipped with,
-- rather than under the outer query's. This test pins the same property one level down: with
-- `parallel_replicas_allow_view_over_mergetree`, a view can expand into a `UNION ALL` *inside* one
-- fragment, and then `createLocalPlanForParallelReplicas` rebuilds several `ReadFromMergeTree` steps
-- at once. Each branch is a `QueryNode` with its own context, and each is planned by its own
-- `Planner` under that context, so on a remote replica every branch looks its read-in-order settings
-- up in its own branch context. Deriving one context from the fragment root would flatten the
-- branches on the initiator only -- the same local-vs-remote divergence, for `UNION ALL` views.
--
-- The two branches of the view below disagree on `read_in_order_use_virtual_row`, and
-- `ReadFromMergeTree::setVirtualRowConversions` consults it on the step's own context, so exactly one
-- of the two rebuilt local steps must carry `Virtual row conversions` -- whatever the outer query says.

DROP TABLE IF EXISTS t_vrow_union_view_a;
DROP TABLE IF EXISTS t_vrow_union_view_b;
DROP VIEW IF EXISTS v_vrow_union_view;

CREATE TABLE t_vrow_union_view_a (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;

CREATE TABLE t_vrow_union_view_b (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;

INSERT INTO t_vrow_union_view_a SELECT number, toString(number) FROM numbers(90000);
INSERT INTO t_vrow_union_view_b SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_vrow_union_view_a FINAL;
OPTIMIZE TABLE t_vrow_union_view_b FINAL;

-- The branches are plain reads, so the whole outer query stays a single parallel-replicas fragment
-- with the view's `UNION ALL` below its sorting step. A branch that carried its own `ORDER BY` would
-- instead become a fragment of its own, and the two branches would never share a rebuild.
CREATE VIEW v_vrow_union_view AS
    SELECT * FROM t_vrow_union_view_a SETTINGS read_in_order_use_virtual_row = 1
    UNION ALL
    SELECT * FROM t_vrow_union_view_b SETTINGS read_in_order_use_virtual_row = 0;

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_allow_view_over_mergetree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_read_in_order = 1;
SET max_threads = 4;
SET explain_query_plan_default = 'legacy';

-- Exactly one branch of the local fragment carries virtual-row conversions, and it is the branch that
-- asked for them. Before the fix both branches followed the outer value: none here, both below.
SELECT 'outer_disables_virtual_row';
SET read_in_order_use_virtual_row = 0;
SELECT countIf(explain LIKE '%Virtual row conversions%')
FROM (EXPLAIN actions = 1 SELECT key FROM v_vrow_union_view ORDER BY key);

SELECT 'outer_enables_virtual_row';
SET read_in_order_use_virtual_row = 1;
SELECT countIf(explain LIKE '%Virtual row conversions%')
FROM (EXPLAIN actions = 1 SELECT key FROM v_vrow_union_view ORDER BY key);

-- Both branches must still be read in order -- the positive control for the assertions above, which
-- would be vacuous if the fragment stopped reading in order altogether.
SELECT 'both_branches_read_in_order';
SELECT countIf(explain LIKE '%ReadType: InOrder%')
FROM (EXPLAIN actions = 1 SELECT key FROM v_vrow_union_view ORDER BY key);

-- Whichever value each branch is built under, the answer must not change. Do not re-sort here.
SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)), count(), sum(key)
FROM (SELECT key FROM v_vrow_union_view ORDER BY key);

DROP VIEW v_vrow_union_view;
DROP TABLE t_vrow_union_view_a;
DROP TABLE t_vrow_union_view_b;
