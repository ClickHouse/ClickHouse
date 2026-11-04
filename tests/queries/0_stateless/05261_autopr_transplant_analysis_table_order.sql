-- Reads in the plan built for automatic parallel replicas are paired with the single-node plan's reads
-- to hand over its index analysis. Pairing them by the name the analyzer gave the table expression
-- does not work: the replicas plan's local plan is built from the tree `buildQueryTreeForShard`
-- produces, which re-runs `createUniqueAliasesIfNecessary` and renumbers `__tableN` from 1 over a tree
-- `inlineAliasColumns` and `rewriteJoinToGlobalJoin` have already rewritten. Where that rewrite
-- changes the traversal order the numbers land on different tables than the initiator gave them, no
-- read pairs up, and the optimization is declined even when the cost model wanted it.
--
-- Three tables with the big one written last is enough to reorder them. The same query with the big
-- table written first is the control: it was never affected, and must stay unaffected.

DROP TABLE IF EXISTS t_order_big;
DROP TABLE IF EXISTS t_order_mid;
DROP TABLE IF EXISTS t_order_small;

-- The layout is pinned because the cost model works off the estimated bytes to read, and leaving the
-- granularity and part format to randomization makes its decision - and with it the point of the
-- test - come and go.
CREATE TABLE t_order_big   (key UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_order_mid   (key UInt64, v UInt64)           ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_order_small (s UInt64, v UInt64)             ENGINE = MergeTree ORDER BY s
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_order_big   SELECT number, number % 500, number FROM numbers(300000);
INSERT INTO t_order_mid   SELECT number, number FROM numbers(300000);
INSERT INTO t_order_small SELECT number, number FROM numbers(500);

-- One part per table, so the plan does not depend on how the insert happened to be split.
OPTIMIZE TABLE t_order_big FINAL;
OPTIMIZE TABLE t_order_mid FINAL;
OPTIMIZE TABLE t_order_small FINAL;

SET enable_analyzer = 1;
-- The reads have to be pruned by their own key conditions - that is what the transplant carries over -
-- and not by a filter built from another side at runtime.
SET enable_join_runtime_filters = 0;
-- Pin the join side and the join order. Left to themselves they are chosen from estimated sizes, and
-- the plan built for parallel replicas estimates differently, being built without index analysis; the
-- two plans then disagree and the optimization gives up for an unrelated reason.
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
-- Randomization turns these off, and without statistics the cost model does not favour replicas for this
-- data at all: the optimization is then never applied and the test measures nothing. Diagnosed with
-- `clickhouse-test --diagnose-random-settings`, which minimized the failure to `use_statistics False`.
SET use_statistics = 1;
SET use_statistics_cache = 1;
-- With a single reading thread the cost model sees replicas as a win on tables of this size, which is
-- what makes the optimization actually apply rather than decline.
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
-- Only the big table is read in parts; each replica reads the other two whole, which on this data is
-- about half of what the query reads, at the default limit of what may be read by every replica.
SET automatic_parallel_replicas_max_duplicated_read_ratio = 1;

-- Baselines planned for a single node.
SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_mid AS m, t_order_small AS s, t_order_big AS b
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    log_comment = '05261_single_node_big_last';

SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_big AS b, t_order_mid AS m, t_order_small AS s
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    log_comment = '05261_single_node_big_first';

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The decision needs dataflow statistics from an earlier execution, so the first run of each shape
-- only collects them and the ones after it are what can act on them.
SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_mid AS m, t_order_small AS s, t_order_big AS b
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS log_comment = '05261_warmup_big_last';

SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_mid AS m, t_order_small AS s, t_order_big AS b
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS log_comment = '05261_replicas_big_last';

SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_mid AS m, t_order_small AS s, t_order_big AS b
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS log_comment = '05261_replicas_big_last';

SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_big AS b, t_order_mid AS m, t_order_small AS s
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS log_comment = '05261_warmup_big_first';

SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_big AS b, t_order_mid AS m, t_order_small AS s
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS log_comment = '05261_replicas_big_first';

SELECT sum(b.v), sum(m.v), sum(s.v) FROM t_order_big AS b, t_order_mid AS m, t_order_small AS s
WHERE m.key = b.key AND s.s = b.s AND b.key < 20000
FORMAT Null SETTINGS log_comment = '05261_replicas_big_first';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Both runs after the warmup must use replicas: a run that quietly stayed single-node is the regression
-- this test is for, and the marks comparison alone would hold for it too.
SELECT
    shape,
    count() = 2 AND min(used > 0) AS replicas_were_used_on_every_run,
    min(marks = single_node_marks) AS every_run_matches_single_node
FROM
(
    SELECT
        if(log_comment = '05261_replicas_big_last', 'big_last', 'big_first') AS shape,
        ProfileEvents['ParallelReplicasUsedCount'] AS used,
        ProfileEvents['SelectedMarks'] AS marks,
        if(log_comment = '05261_replicas_big_last',
           (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
              AND event_date >= yesterday() AND log_comment = '05261_single_node_big_last'),
           (SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
            WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
              AND event_date >= yesterday() AND log_comment = '05261_single_node_big_first')) AS single_node_marks
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
      AND event_date >= yesterday()
      AND log_comment IN ('05261_replicas_big_last', '05261_replicas_big_first')
)
GROUP BY shape
ORDER BY shape
FORMAT TSVWithNames;

DROP TABLE t_order_big;
DROP TABLE t_order_mid;
DROP TABLE t_order_small;
