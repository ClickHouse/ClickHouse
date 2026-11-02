-- A read from a `Merge` table always falls back to local execution on `ReadFromMerge` (never serializable), but every
-- child plan decides for itself and, when accepted, executes as distributed-plan tasks (`main` / `stage_*` rows in
-- `system.query_log`). The exception is a child that references an `IN (SELECT ...)` set of the outer query: only the
-- plan owning a set prepares it for the tasks, so such a child stays local (`queryHasSubquerySets` in `StorageMerge`).
-- A set the child owns itself (inside a `View` child) or a tuple set is shipped. The rows are identical either way, so
-- the verdicts are read from `query_log`: the child tasks inherit the statement's `log_comment`.

DROP TABLE IF EXISTS m_05218;
DROP TABLE IF EXISTS mv_05218;
DROP TABLE IF EXISTS vw_05218;
DROP TABLE IF EXISTS t_05218;

CREATE TABLE t_05218 (k Int, v Int) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05218 SELECT number, number FROM numbers(10000);
CREATE VIEW vw_05218 AS SELECT k, v FROM t_05218 WHERE k IN (SELECT number FROM numbers(100));
CREATE TABLE m_05218 ENGINE = Merge(currentDatabase(), '^t_05218$');
CREATE TABLE mv_05218 ENGINE = Merge(currentDatabase(), '^vw_05218$');

-- max_rows_to_group_by must be 0: the CI profile sets a limit and make_distributed_plan rejects aggregation with one.
-- distributed_plan_max_rows_to_broadcast = 0 forces bucketed reads, so an accepted child always has tasks.
SET make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 1, enable_parallel_replicas = 0,
    distributed_plan_execute_locally = 1, max_rows_to_group_by = 0, prefer_localhost_replica = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3;

SELECT count() FROM m_05218 WHERE v > 5 SETTINGS log_comment = '05218 1 no sets: child tasks';
SELECT count() FROM m_05218 WHERE v IN (SELECT number FROM numbers(100)) SETTINGS log_comment = '05218 2 outer IN set on a non-key column: local';
SELECT count() FROM m_05218 WHERE k IN (SELECT number FROM numbers(100)) SETTINGS log_comment = '05218 3 outer IN set on the key column: local';
SELECT count() FROM m_05218 WHERE 1 IN (SELECT 1) SETTINGS log_comment = '05218 4 outer IN set not referencing the table: local';
SELECT count() FROM m_05218 WHERE k GLOBAL IN (SELECT number FROM numbers(100)) SETTINGS log_comment = '05218 5 outer GLOBAL IN set: local';
SELECT count() FROM m_05218 WHERE k IN (1, 2, 3) SETTINGS log_comment = '05218 6 tuple set: child tasks';
SELECT count() FROM mv_05218 SETTINGS log_comment = '05218 7 View child owning its IN set: child tasks';

-- The verification below must not itself run as a distributed plan: `system.query_log` receives new parts
-- while it runs, and a bucketed distributed read pinned to the coordinator's part list then fails with
-- `NO_SUCH_DATA_PART`. Turn the setting off before it.
SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;
-- Only rows of this run: the test's own `Merge` table is created at the start of the run.
WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 'm_05218') AS run_start
SELECT log_comment, countIf(query = 'main' OR query LIKE 'stage\_%') > 0 AS children_executed_remote_tasks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= toDate(run_start) AND event_time >= run_start AND log_comment LIKE '05218 %'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE m_05218;
DROP TABLE mv_05218;
DROP TABLE vw_05218;
DROP TABLE t_05218;
