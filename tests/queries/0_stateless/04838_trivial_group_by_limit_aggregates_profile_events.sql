-- Verifies that the trivial `GROUP BY ... LIMIT` optimization actually does work for a
-- projection with aggregate functions: when it fires, the aggregator caps the number of
-- keys (`OverflowAny`) and the parallel streams rebuild their hash tables to the shared
-- set of kept keys (`AggregationSharedKeptKeysRebuilds`). When the optimization is
-- disabled, neither event fires because no limit is set.
--
-- `enable_parallel_replicas = 0` is pinned because with parallel replicas the aggregation
-- is split between the replicas and the initiator (`isSecondStage` is false on the
-- replicas), so the cutoff intentionally stays off there.

DROP TABLE IF EXISTS t_04838;
CREATE TABLE t_04838 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04838 SELECT number, number FROM numbers(100000);

SELECT k, count() AS c, sum(v) AS s FROM t_04838 GROUP BY k LIMIT 5 FORMAT Null
SETTINGS optimize_trivial_group_by_limit_query = 1, max_threads = 4,
    enable_parallel_replicas = 0, log_comment = '04838_agg_on';

SELECT k, count() AS c, sum(v) AS s FROM t_04838 GROUP BY k LIMIT 5 FORMAT Null
SETTINGS optimize_trivial_group_by_limit_query = 0, max_threads = 4,
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

DROP TABLE t_04838;
