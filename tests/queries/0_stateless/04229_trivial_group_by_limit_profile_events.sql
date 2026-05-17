-- Verifies that `OptimizeTrivialGroupByLimitPass` actually does work:
-- when it fires, the aggregator's `OverflowAny` `ProfileEvent` is incremented
-- (the aggregator drops new keys after `max_rows_to_group_by` is reached);
-- when it is disabled, `OverflowAny` stays at zero because no limit is set.

DROP TABLE IF EXISTS t_04229;
CREATE TABLE t_04229 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04229 SELECT number FROM numbers(100000);

-- Pin to a single thread so all the aggregation happens in one variant, making
-- the `OverflowAny` count deterministic. The query has 100k distinct keys, so
-- with the optimization the aggregator hits the 5-key cap and drops all the
-- subsequent keys, incrementing `OverflowAny`.

SELECT k FROM t_04229 GROUP BY k LIMIT 5 FORMAT Null
SETTINGS optimize_trivial_group_by_limit_query = 1, max_threads = 1, log_comment = '04229_on';

SELECT k FROM t_04229 GROUP BY k LIMIT 5 FORMAT Null
SETTINGS optimize_trivial_group_by_limit_query = 0, max_threads = 1, log_comment = '04229_off';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['OverflowAny'] > 0 AS overflow_any_fired
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('04229_on', '04229_off')
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_04229;
