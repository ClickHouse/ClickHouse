-- Tags: no-parallel-replicas
-- Coverage for the planner-side cost of insert-time statistics under the shipped defaults.
--
-- With `materialize_statistics_on_insert` enabled by default, a freshly loaded small table
-- already carries statistics in its parts, so `optimizeJoin` loads them while estimating the
-- relations of a plain two-table join. This test pins that behavior: the load happens and is
-- attributed to the `LoadedStatisticsMicroseconds` profile event when `use_statistics` is on,
-- and does not happen when it is off.
--
-- `MergeTreeData::getConditionSelectivityEstimator` does not populate `cached_estimator` on the
-- query-time path - only the background task behind `refresh_statistics_interval` does - so with
-- the default `use_statistics_cache = 1` the load is paid by every query until that task fires.
-- The measured queries below pin `use_statistics_cache = 0` so the result does not depend on the
-- timing of that background task.

SET allow_statistics = 1;
SET enable_analyzer = 1;
SET use_statistics = 1;
SET async_insert = 0;
-- Pinned to the shipped defaults, which `clickhouse-test` otherwise randomizes.
SET materialize_statistics_on_insert = 1;
SET materialize_statistics_on_insert_max_table_size = 26843545600;

DROP TABLE IF EXISTS t_stats_cold_load_fact;
DROP TABLE IF EXISTS t_stats_cold_load_dim;

CREATE TABLE t_stats_cold_load_fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_stats_cold_load_dim (id UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

SYSTEM STOP MERGES t_stats_cold_load_fact;
SYSTEM STOP MERGES t_stats_cold_load_dim;

-- Both tables are far below `materialize_statistics_on_insert_max_table_size`, so the parts
-- written here carry the statistics selected by the default `auto_statistics_types`.
INSERT INTO t_stats_cold_load_fact SELECT number, number * 2 FROM numbers(10000);
INSERT INTO t_stats_cold_load_dim SELECT number, toString(number) FROM numbers(1000);

SELECT 'statistics materialized on insert', count(), min(length(statistics)) > 0
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table IN ('t_stats_cold_load_fact', 't_stats_cold_load_dim')
    AND active AND column = 'id';

SELECT count() FROM t_stats_cold_load_fact AS f JOIN t_stats_cold_load_dim AS d ON f.id = d.id
SETTINGS log_comment = '04650_cold_load_on', use_statistics = 1, use_statistics_cache = 0,
    query_plan_optimize_join_order_limit = 10;

SELECT count() FROM t_stats_cold_load_fact AS f JOIN t_stats_cold_load_dim AS d ON f.id = d.id
SETTINGS log_comment = '04650_cold_load_off', use_statistics = 0, use_statistics_cache = 0,
    query_plan_optimize_join_order_limit = 10;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['LoadedStatisticsMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment IN ('04650_cold_load_on', '04650_cold_load_off')
ORDER BY log_comment;

DROP TABLE t_stats_cold_load_fact;
DROP TABLE t_stats_cold_load_dim;
