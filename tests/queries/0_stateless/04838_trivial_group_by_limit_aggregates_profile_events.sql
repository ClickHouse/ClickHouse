-- Verifies that the trivial `GROUP BY ... LIMIT` optimization actually does work for a
-- projection with aggregate functions: when it fires, the aggregator caps the number of
-- keys (`OverflowAny`) and the parallel streams rebuild their hash tables to the shared
-- set of kept keys (`AggregationSharedKeptKeysRebuilds`). When the optimization is
-- disabled, neither event fires because no limit is set.
--
-- The row count and `max_block_size` are chosen so `numbers_mt` splits the read into
-- several streams (with too few blocks the read collapses into a single stream, where
-- the per-stream cutoff is already exact and no rebuild is needed) and the aggregation
-- is guaranteed to be parallel, so the shared cutoff engages and the streams rebuild.
--
-- `enable_parallel_replicas = 0` is pinned because with parallel replicas the aggregation
-- is split between the replicas and the initiator (`isSecondStage` is false on the
-- replicas), so the cutoff intentionally stays off there.
--
-- `enable_analyzer = 1` is pinned because the aggregate cutoff is armed by the planner of
-- the analyzer; with the old analyzer the events never fire.
SET enable_analyzer = 1;

SELECT toUInt64(number) AS k, count() AS c, sum(number) AS s FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS optimize_trivial_group_by_limit_query = 1, max_threads = 4, max_block_size = 8192,
    enable_parallel_replicas = 0, log_comment = '04838_agg_on';

SELECT toUInt64(number) AS k, count() AS c, sum(number) AS s FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS optimize_trivial_group_by_limit_query = 0, max_threads = 4, max_block_size = 8192,
    enable_parallel_replicas = 0, log_comment = '04838_agg_off';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['OverflowAny'] > 0 AS overflow_any_fired,
    ProfileEvents['AggregationSharedKeptKeysRebuilds'] > 0 AS kept_keys_rebuilds_fired
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('04838_agg_on', '04838_agg_off')
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;
