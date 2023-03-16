-- Tags: no-random-settings, no-parallel, no-s3-storage

DROP TABLE IF EXISTS t_sharded_map_2;

CREATE TABLE t_sharded_map_2 (id UInt64, m Map(String, UInt64, 4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_sharded_map_2 SELECT number, (arrayMap(x -> 'c_' || x::String, range(number % 10)), range(number % 10)) FROM numbers(10);

SET allow_experimental_analyzer = 1;
SYSTEM DROP MARK CACHE;
SELECT m['c_5'] AS v, count() FROM t_sharded_map_2 GROUP BY v;

SET allow_experimental_analyzer = 0;
SYSTEM DROP MARK CACHE;
SELECT m['c_5'] AS v, count() FROM t_sharded_map_2 GROUP BY v;

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['FileOpen'] FROM system.query_log
WHERE current_database = currentDatabase() AND query ILIKE 'SELECT%FROM%t_sharded_map_2%' AND type = 'QueryFinish';

DROP TABLE t_sharded_map_2;
