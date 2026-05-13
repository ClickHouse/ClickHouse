SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics_cache = 0;

DROP TABLE IF EXISTS t_lazy;
DROP TABLE IF EXISTS t_lazy_narrow;

-- Wide-part table: each case touches a different subset of stats columns to prove
-- only the requested ones are read.
CREATE TABLE t_lazy (a UInt64, b UInt64, c UInt64, d UInt64, e UInt64, k UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'minmax',
         refresh_statistics_interval = 0;

-- Compact-part table (default min_bytes_for_wide_part), exercises the same stats
-- code path through compact storage on the right side of the JOIN case.
CREATE TABLE t_lazy_narrow (k UInt64, v UInt64, x UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax',
         refresh_statistics_interval = 0;

INSERT INTO t_lazy SELECT number, number, number, number, number, number FROM numbers(1000);
INSERT INTO t_lazy SELECT number + 2000000, number, number, number, number, number FROM numbers(1000);
INSERT INTO t_lazy_narrow SELECT number, number % 10, number FROM numbers(1000);

SELECT name, part_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_lazy', 't_lazy_narrow') AND active
ORDER BY table, name;

-- INSERT pre-populates the per-part estimates cache; drop it so the SELECTs below
-- measure real disk loads via `LoadedStatisticsColumns`.
DETACH TABLE t_lazy;
ATTACH TABLE t_lazy;
DETACH TABLE t_lazy_narrow;
ATTACH TABLE t_lazy_narrow;

-- Case 1: part pruning, filter on `a` -> 1 column x 2 wide parts = 2.
SELECT count() FROM t_lazy WHERE a > 1000000
SETTINGS use_statistics_for_part_pruning = 1, use_statistics = 0, log_comment = '04209_part_lazy'
FORMAT Null;

-- Case 2: PREWHERE, two conjuncts on `b` -> 1 column x 2 wide parts = 2.
SELECT b, c, d, e FROM t_lazy WHERE b = 42 AND b >= 0
SETTINGS use_statistics_for_part_pruning = 0, use_statistics = 1,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         log_comment = '04209_prewhere_lazy'
FORMAT Null;

-- Case 3: JOIN reorder. Wide side `t_lazy.k` (1 col x 2 parts = 2) +
-- compact side `t_lazy_narrow.k` and `v` (2 cols x 1 part = 2) = 4.
SELECT count() FROM t_lazy AS w INNER JOIN t_lazy_narrow AS n ON w.k = n.k WHERE n.v = 1
SETTINGS use_statistics_for_part_pruning = 0, use_statistics = 1,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         query_plan_optimize_join_order_limit = 10,
         log_comment = '04209_join_lazy'
FORMAT Null;

-- Case 4: JOIN reorder with casted keys. Wide side `t_lazy.c` (1 col x 2 parts = 2) +
-- compact side `t_lazy_narrow.x` (1 col x 1 part = 1) = 3.
SELECT count() FROM t_lazy AS w INNER JOIN t_lazy_narrow AS n ON CAST(w.c AS Int64) = toInt64(n.x)
SETTINGS use_statistics_for_part_pruning = 0, use_statistics = 1,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         query_plan_optimize_join_order_limit = 10,
         log_comment = '04209_alias_join_lazy'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, toUInt64(ProfileEvents['LoadedStatisticsColumns'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04209_part_lazy', '04209_prewhere_lazy', '04209_join_lazy', '04209_alias_join_lazy')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

DROP TABLE t_lazy;
DROP TABLE t_lazy_narrow;
