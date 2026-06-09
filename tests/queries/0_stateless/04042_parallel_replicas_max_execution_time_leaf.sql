-- Tags: no-fasttest, no-old-analyzer
-- Tag no-fasttest: parallel replicas require a cluster that is not configured in the fast test.
-- Tag no-old-analyzer: parallel reading from replicas is only built on the analyzer code path, so
-- 'max_execution_time_leaf' does not apply with the old analyzer (the query runs as a plain local read).

DROP TABLE IF EXISTS test_max_execution_time_leaf SYNC;
CREATE TABLE test_max_execution_time_leaf
(
    key UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_max_execution_time_leaf', 'r1')
ORDER BY key
SETTINGS index_granularity = 10;

SET max_rows_to_read = 0;
INSERT INTO test_max_execution_time_leaf SELECT number FROM numbers(1000);

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SET use_query_cache = false;
-- Disable the automatic parallel replicas path so the explicit settings above are honoured (it would otherwise
-- override the cluster).
SET automatic_parallel_replicas_mode = 0;

-- Note: 'parallel_replicas_local_plan' is intentionally left at its default (1). When 'max_execution_time_leaf'
-- is set, the local plan is disabled automatically (the local replica shares the initiator's query status and
-- cannot be bounded by the leaf timeout separately), so the leaf timeout is effective for the default path too.

-- 'sleepEachRow' with 'max_block_size = 1' makes every replica spend a deterministic amount of wall-clock time while
-- reading, so the timeout fires reliably regardless of how fast the hardware is. The work is spread across replicas,
-- so each leaf accumulates several seconds of sleep, far exceeding the one second timeout.

-- The whole-query timeout 'max_execution_time' aborts the query.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf SETTINGS max_block_size = 1, max_execution_time = 1; -- { serverError TIMEOUT_EXCEEDED }
-- The leaf timeout 'max_execution_time_leaf' aborts the per-replica (leaf) execution.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf SETTINGS max_block_size = 1, max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED }
-- In 'break' mode a partial result is returned instead of an error.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf FORMAT Null SETTINGS max_block_size = 1, max_execution_time = 1, timeout_overflow_mode = 'break';
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf FORMAT Null SETTINGS max_block_size = 1, max_execution_time_leaf = 1, timeout_overflow_mode_leaf = 'break';

DROP TABLE test_max_execution_time_leaf SYNC;
