SET log_queries = 1, log_queries_min_type = 'QUERY_FINISH';
SET optimize_use_projections = 1;
-- Under a Replicated database the DDL below would otherwise print a per-host status row.
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_05023_1;
DROP TABLE IF EXISTS t_05023_2;
DROP TABLE IF EXISTS t_05023_m;

CREATE TABLE t_05023_1 (a UInt32, b UInt32, c UInt32, PROJECTION p_c (SELECT * ORDER BY c))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;

CREATE TABLE t_05023_2 (a UInt32, b UInt32, c UInt32, PROJECTION p_c (SELECT * ORDER BY c))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;

INSERT INTO t_05023_1 SELECT number, number * 2, number * 3 FROM numbers(1000);
INSERT INTO t_05023_2 SELECT number, number * 2, number * 3 FROM numbers(1000);

CREATE TABLE t_05023_m AS t_05023_1 ENGINE = Merge(currentDatabase(), '^t_05023_[12]$');

SELECT sum(b) FROM t_05023_m WHERE c > 2000 SETTINGS log_comment = '05023_merge_engine_projection';

SYSTEM FLUSH LOGS query_log;

-- The database name is random per run, so strip it from the qualified projection names.
SELECT arraySort(arrayMap(x -> substring(x, position(x, '.') + 1), projections))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '05023_merge_engine_projection'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_05023_m;
DROP TABLE t_05023_1;
DROP TABLE t_05023_2;
