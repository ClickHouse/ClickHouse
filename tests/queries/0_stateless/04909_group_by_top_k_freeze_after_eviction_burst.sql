-- The profitability freeze tolerates a single eviction burst: evictions come in
-- trims of ~capacity/2, so a heap that trimmed once early and then went quiet
-- (`evicted_keys < capacity`) is pure overhead and freezes at the end of the
-- observation window.  A heap that keeps evicting accumulates far more than
-- its capacity within the window and must stay alive: pruning the hash table
-- is the memory win.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET group_by_top_k_optimization_observation_rows = 65536;
-- One stream, so the assertions below describe a single heap.
SET max_threads = 1;
SET enable_parallel_replicas = 0;
-- In-order aggregation bypasses the heap (and its randomized in-order read was
-- slow enough to trip `TOO_SLOW` in the flaky check); pin the batch path.
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS t_freeze_burst;
CREATE TABLE t_freeze_burst (k UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Fill the heap to its capacity of 1000, then one burst of 501 better keys:
-- the heap grows past its trim threshold (1500) exactly once and evicts ~501
-- keys.  The remaining rows all hit the best surviving group, so the heap
-- neither skips nor evicts through the rest of the observation window.
-- Everything goes into a single part in a single insert block: parts can be
-- read in any order, and the phases must reach the aggregation in this order.
SET max_insert_threads = 1;
SET min_insert_block_size_rows = 1000000;
INSERT INTO t_freeze_burst
SELECT multiIf(number < 1000, 10000 + number, number < 1501, 1 + (number - 1000), 1)
FROM numbers(501501);

SELECT k, count() FROM t_freeze_burst GROUP BY k ORDER BY k ASC LIMIT 1000
SETTINGS log_comment = '04909_burst_then_quiet' FORMAT Null;

DROP TABLE t_freeze_burst;

-- Sustained eviction: every row is a new key that is better than the boundary,
-- so the heap admits and evicts continuously, far past its capacity.
DROP TABLE IF EXISTS t_freeze_churn;
CREATE TABLE t_freeze_churn (k UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_freeze_churn SELECT 1000000 - number FROM numbers(200000) SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000;

SELECT k, count() FROM t_freeze_churn GROUP BY k ORDER BY k ASC LIMIT 1000
SETTINGS log_comment = '04909_sustained_eviction' FORMAT Null;

DROP TABLE t_freeze_churn;

SYSTEM FLUSH LOGS query_log;

SELECT 'burst then quiet: frozen after one trim';
SELECT
    max(ProfileEvents['AggregationTopKHeapsFrozen']),
    max(ProfileEvents['AggregationTopKKeysEvicted']) BETWEEN 1 AND 999
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04909_burst_then_quiet';

SELECT 'sustained eviction: never frozen';
SELECT
    max(ProfileEvents['AggregationTopKHeapsFrozen']),
    max(ProfileEvents['AggregationTopKKeysEvicted']) > 100000
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04909_sustained_eviction';
