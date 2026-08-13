SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_lazy;

-- Wide-part table: the part-pruning path must load statistics only for the filter
-- columns that carry them, not for every column of the part.
CREATE TABLE t_lazy (a UInt64, b UInt64, c UInt64, d UInt64, e UInt64, k UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'basic',
         refresh_statistics_interval = 0;

INSERT INTO t_lazy SELECT number, number, number, number, number, number FROM numbers(1000);
INSERT INTO t_lazy SELECT number + 2000000, number, number, number, number, number FROM numbers(1000);

SELECT name, part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_lazy' AND active
ORDER BY table, name;

-- INSERT pre-populates the per-part estimates cache; drop it so the SELECTs below
-- measure real disk loads via `LoadedStatisticsColumns`.
DETACH TABLE t_lazy;
ATTACH TABLE t_lazy;

-- Part pruning, filter on `a` -> 1 column x 2 wide parts = 2.
SELECT count() FROM t_lazy WHERE a > 1000000
SETTINGS use_statistics_for_part_pruning = 1, use_statistics = 0, log_comment = '04209_part_lazy'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, toUInt64(ProfileEvents['LoadedStatisticsColumns'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04209_part_lazy')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

DROP TABLE t_lazy;
