-- Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
-- no-parallel: enables a server-global failpoint on the deduplication-conflict path.
-- no-shared-merge-tree: StorageSharedMergeTree has its own sink.
-- no-replicated-database: the table is created with an explicit replica name.

-- A concurrent insert can re-create the deduplication node that a conflict resolution has just
-- found gone, so the resolution can repeat. Every repeat has to reach the insert's retry
-- controller, which counts it against insert_keeper_max_retries: a workload that keeps
-- re-creating the node has to end the insert with an error rather than keep it running.
-- The failpoint below reports every conflict as vanished, which is what the sink observes under
-- such a workload. The concurrent carriers that produce a single vanished node are covered by
-- 04953 (refused lock request) and 04954 (rejected commit transaction).

SYSTEM DISABLE FAILPOINT rmt_dedup_conflict_node_missing;

DROP TABLE IF EXISTS t_04955 SYNC;

CREATE TABLE t_04955 (k UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04955/t', 'r1')
ORDER BY k;

-- deduplicate_insert overrides insert_deduplicate, so it is the one that has to be set here.
-- insert_keeper_max_retries = 0 makes the first counted repeat the last one.
SET async_insert = 0, deduplicate_insert = 'enable',
    insert_keeper_fault_injection_probability = 0, insert_keeper_max_retries = 0;

-- Registers the deduplication hash in Keeper. Nothing removes it, so every lock request below is
-- refused and every resolution has a conflict to report.
INSERT INTO t_04955 VALUES (1);

-- The node is there and the resolution finds it, so the duplicate is deduplicated.
INSERT INTO t_04955 VALUES (1);
SELECT 'deduplicated while the node is visible', count() FROM t_04955;

SYSTEM ENABLE FAILPOINT rmt_dedup_conflict_node_missing;

-- Bounds the query_log check below to this execution: some jobs run the suite in one fixed
-- database, and a failed test is re-run in it, so an earlier execution's rows are still there.
-- Not a temporary table: the failing insert below discards the session's temporary tables.
DROP TABLE IF EXISTS started_04955;
CREATE TABLE started_04955 ENGINE = Memory AS SELECT now64(6) AS at;

-- The first resolution reports the node gone and retries the lock request, which the same node
-- refuses again. The second resolution is the repeat the retry controller has to stop.
INSERT INTO t_04955 VALUES (1); -- { serverError UNFINISHED }

SYSTEM DISABLE FAILPOINT rmt_dedup_conflict_node_missing;

SELECT 'rows after the bounded insert', count() FROM t_04955;

-- UNFINISHED alone would also accept an unrelated failure, so pin the retry controller's message.
SYSTEM FLUSH LOGS query_log;
SELECT 'bounded by the retry controller', count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time_microseconds >= (SELECT at FROM started_04955)
  AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
  AND exception LIKE '%keep being created and removed%'
SETTINGS max_rows_to_read = 0;

DROP TABLE t_04955 SYNC;
DROP TABLE started_04955;
