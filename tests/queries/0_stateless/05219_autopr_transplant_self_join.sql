-- Handing the single-node index analysis to the reads of the plan built for automatic parallel
-- replicas means pairing the reads of the two plans, and the two plans are optimized separately, so
-- position cannot do it. Reads are paired by the table they read together with the name the analyzer
-- gave the table expression (`__table1`, `__table2`, ...). The table alone would be enough for a join
-- of two different tables; a query that reads one table twice is what the name is for, and what this
-- test covers.
--
-- The two aliases select very different ranges, so a pairing that crossed them - or gave up and left
-- a read unanalyzed - shows up as marks the single-node plan did not read.

DROP TABLE IF EXISTS t_transplant_self;

-- The layout is pinned because the cost model that decides whether replicas pay off works off the
-- estimated bytes to read, and leaving the granularity and part format to randomization makes that
-- decision - and with it the whole point of the test - come and go.
CREATE TABLE t_transplant_self (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_transplant_self SELECT number, number FROM numbers(300000);

-- One part, so that the plan the decision is matched against does not depend on how the insert
-- happened to be split.
OPTIMIZE TABLE t_transplant_self FINAL;

SET enable_analyzer = 1;
-- Each side has to be pruned by its own key condition - that is what the transplant must carry over -
-- and not by a filter built from the other side at runtime.
SET enable_join_runtime_filters = 0;
-- Pin the join side. Left to 'auto' it is chosen from estimated table sizes, and the plan built for
-- parallel replicas estimates differently from the single-node plan, being built without index
-- analysis. The two then disagree on which side to build, and the optimization gives up because it
-- can no longer match one plan against the other.
SET query_plan_join_swap_table = 'false';
-- With a single reading thread the cost model sees replicas as a win on a table of this size, which
-- is what makes the optimization actually apply rather than decline.
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- Baseline: the same query planned for a single node.
SELECT sum(l.v), sum(r.v)
FROM t_transplant_self AS l INNER JOIN t_transplant_self AS r ON l.key = r.key
WHERE l.key < 250000 AND r.key < 20000
FORMAT Null SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    log_comment = '05219_single_node';

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The decision needs dataflow statistics from an earlier execution, so the first run only collects
-- them and the ones after it are what can act on them.
SELECT sum(l.v), sum(r.v)
FROM t_transplant_self AS l INNER JOIN t_transplant_self AS r ON l.key = r.key
WHERE l.key < 250000 AND r.key < 20000
FORMAT Null SETTINGS log_comment = '05219_warmup';

SELECT sum(l.v), sum(r.v)
FROM t_transplant_self AS l INNER JOIN t_transplant_self AS r ON l.key = r.key
WHERE l.key < 250000 AND r.key < 20000
FORMAT Null SETTINGS log_comment = '05219_with_replicas';

SELECT sum(l.v), sum(r.v)
FROM t_transplant_self AS l INNER JOIN t_transplant_self AS r ON l.key = r.key
WHERE l.key < 250000 AND r.key < 20000
FORMAT Null SETTINGS log_comment = '05219_with_replicas';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- `replicas_were_used_at_least_once` is what keeps this honest: without it the comparison would also
-- hold for a run that quietly stayed single-node, and the test would pass while testing nothing.
WITH
    (SELECT ProfileEvents['SelectedMarks']
     FROM system.query_log
     WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
       AND event_date >= yesterday() AND log_comment = '05219_single_node') AS single_node_marks,
    replica_runs AS
    (
        SELECT ProfileEvents['ParallelReplicasUsedCount'] AS used, ProfileEvents['SelectedMarks'] AS marks
        FROM system.query_log
        WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
          AND event_date >= yesterday() AND log_comment = '05219_with_replicas'
    )
SELECT
    countIf(used > 0) > 0 AS replicas_were_used_at_least_once,
    countIf(used > 0 AND marks != single_node_marks) = 0 AS every_replica_run_matches_single_node
FROM replica_runs
FORMAT TSVWithNames;

DROP TABLE t_transplant_self;
