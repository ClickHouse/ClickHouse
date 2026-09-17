-- Distributing the child plans of a `Merge` table under `make_distributed_plan` is supported
-- (see 04367_distributed_plan_merge_scatter_multishard), with one exception: when the query has
-- subquery sets (`IN (SELECT ...)`). A `Merge` table materializes its child plans lazily, while
-- the outer plan is already executing, so the outer plan's `addStepsToBuildSets` has already
-- moved the source plan out of every `FutureSetFromSubquery`, and a distributed child fragment
-- referencing such a set failed to serialize with the logical error
-- `Cannot serialize FutureSetFromSubquery with no query plan`. Such child plans must stay local.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/112849

DROP TABLE IF EXISTS t_merge_dp;
DROP TABLE IF EXISTS m_merge_dp;
CREATE TABLE t_merge_dp (x UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Large enough that the distributed conversion does not keep the read local by the size heuristic.
INSERT INTO t_merge_dp SELECT number FROM numbers(100000);
CREATE TABLE m_merge_dp (x UInt64) ENGINE = Merge(currentDatabase(), '^t_merge_dp$');

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0 (randomized
-- settings set it nonzero, which would make make_distributed_plan reject queries at planning time).
-- `make_distributed_plan` itself is attached per query, not SET for the session: the queries are
-- plain SELECTs (and EXPLAIN wrappers) because converting an *outer* plan whose worker stage would
-- carry an unserializable step (`ReadFromMerge` under aggregation or ORDER BY, `ReadFromStorage`
-- of the EXPLAIN wrapper) is rejected upfront (`SUPPORT_IS_DISABLED`) - a pre-existing limitation
-- unrelated to the child plans. `explain_query_plan_default` is pinned so the EXPLAIN checks see
-- the legacy step names.
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
    explain_query_plan_default = 'legacy';
-- A single stream, so the row order of a local child read is deterministic.
SET max_threads = 1;

-- Positive control: without subquery sets the child plan is distributed (contains exchanges), and
-- executing it applies the pushed-down filter (exactly one row survives).
SELECT sum(explain LIKE '%Exchange%') > 0 FROM (EXPLAIN SELECT x FROM m_merge_dp WHERE x = 3 SETTINGS make_distributed_plan = 1);
SELECT x FROM m_merge_dp WHERE x = 3 SETTINGS make_distributed_plan = 1;

-- With a subquery set the child plan stays local (no exchanges) and the query succeeds.
SELECT sum(explain LIKE '%Exchange%') FROM (EXPLAIN SELECT x FROM m_merge_dp WHERE x GLOBAL IN (SELECT number FROM numbers(3)) SETTINGS make_distributed_plan = 1);
SELECT x FROM m_merge_dp WHERE x GLOBAL IN (SELECT number FROM numbers(3)) SETTINGS make_distributed_plan = 1;

SYSTEM FLUSH LOGS query_log;
-- The child plans executed as distributed-plan tasks (`main`, `stage_*`) of the statements above. The outer plan
-- falls back on `ReadFromMerge` by design, so a lost child distribution shows only here, not in the rows.
-- Only rows of this run: the test's own `Merge` table is created at the start of the run.
WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 'm_merge_dp') AS run_start
SELECT countIf(query = 'main' OR query LIKE 'stage\_%') > 0 AS children_executed_distributed
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= toDate(run_start) AND event_time >= run_start
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND event_date >= toDate(run_start) AND event_time >= run_start AND is_initial_query
            AND current_database = currentDatabase() AND query LIKE 'SELECT x FROM m\_merge\_dp WHERE x = 3%');
-- With a subquery set the child plans stayed local: no task at all for that statement.
-- Only rows of this run: the test's own `Merge` table is created at the start of the run.
WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 'm_merge_dp') AS run_start
SELECT countIf(query = 'main' OR query LIKE 'stage\_%') = 0 AS children_stayed_local
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= toDate(run_start) AND event_time >= run_start
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE type = 'QueryFinish' AND event_date >= toDate(run_start) AND event_time >= run_start AND is_initial_query
            AND current_database = currentDatabase() AND query LIKE 'SELECT x FROM m\_merge\_dp WHERE x GLOBAL IN%');

DROP TABLE m_merge_dp;
DROP TABLE t_merge_dp;
