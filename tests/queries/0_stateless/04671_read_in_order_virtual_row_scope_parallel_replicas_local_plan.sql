-- Tags: no-random-settings, no-random-merge-tree-settings

-- The initiator-local fragment of a parallel-replicas read is the initiator's share of the very
-- same fragment the remote replicas run, so it must be built under the settings the fragment is
-- shipped with. `buildQueryPlanForParallelReplicas` passes `planner_context->getQueryContext()` --
-- the *outer* context, even when the fragment is a subquery with its own `SETTINGS` -- down to
-- `createLocalPlanForParallelReplicas`, and `createLocalParallelReplicasReadingStep` stamps that
-- context onto the rebuilt `ReadFromMergeTree`. Every read-in-order setting lookup the step makes
-- goes through that context, so a subquery-scoped one used to be honoured by the remote replicas
-- (they re-plan the shipped fragment under it) but not by the initiator.
--
-- `read_in_order_use_virtual_row` is the directly observable one:
-- `ReadFromMergeTree::setVirtualRowConversions` consults it on the step context, so
-- `Virtual row conversions` shows up on the rebuilt local step only when the *fragment's* own
-- value is on. This is the same class of divergence that
-- `04650_limit_by_push_down_parallel_replicas_local_plan` pins on the optimizer-settings side.
--
-- The outer plan holds only the local fragment and `ReadFromRemoteParallelReplicas`, so any
-- `Virtual row conversions` in the plan below necessarily belongs to the local fragment.

DROP TABLE IF EXISTS t_vrow_scope_pr_local;

CREATE TABLE t_vrow_scope_pr_local (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;

INSERT INTO t_vrow_scope_pr_local SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_vrow_scope_pr_local FINAL;

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_read_in_order = 1;
SET max_threads = 4;

-- The outer query turns virtual rows off, the shipped subquery turns them on: the rebuilt local
-- step must follow the subquery. Before the fix it evaluated the outer `0` and carried no
-- conversions, while the remote replicas used them.
SET read_in_order_use_virtual_row = 0;

SELECT 'subquery_enables_virtual_row';
SELECT count() >= 1 FROM (
    EXPLAIN actions = 1 SELECT * FROM (
        SELECT * FROM t_vrow_scope_pr_local WHERE value LIKE '%5%' ORDER BY key
        SETTINGS read_in_order_use_virtual_row = 1
    )
) WHERE explain LIKE '%Virtual row conversions%';

-- The mirror image: the outer query turns virtual rows on, the shipped subquery turns them off.
-- Before the fix the local step carried conversions the replicas did not have.
SET read_in_order_use_virtual_row = 1;

SELECT 'subquery_disables_virtual_row';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1 SELECT * FROM (
        SELECT * FROM t_vrow_scope_pr_local WHERE value LIKE '%5%' ORDER BY key
        SETTINGS read_in_order_use_virtual_row = 0
    )
) WHERE explain LIKE '%Virtual row conversions%';

-- Whichever value the fragment is built under, the read must stay in order and the answer must
-- not change. Do not re-sort here -- that would mask reordering.
SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)), count(), sum(key) FROM (
    SELECT * FROM (
        SELECT key FROM t_vrow_scope_pr_local ORDER BY key
        SETTINGS read_in_order_use_virtual_row = 1
    )
);
SELECT groupArray(key) = arraySort(groupArray(key)), count(), sum(key) FROM (
    SELECT * FROM (
        SELECT key FROM t_vrow_scope_pr_local ORDER BY key
        SETTINGS read_in_order_use_virtual_row = 0
    )
);

DROP TABLE t_vrow_scope_pr_local;
