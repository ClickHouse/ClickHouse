-- `TTL GROUP BY ... SET` assigns a new value to a column of the sorting key,
-- so the aggregated rows may become out of order (https://github.com/ClickHouse/ClickHouse/issues/108514).
-- Then the sorting key of such a row is replaced with the sorting key of the previous row, and the part stays sorted.

DROP TABLE IF EXISTS t_ttl_group_by_set_sorting_key;

CREATE TABLE t_ttl_group_by_set_sorting_key (k Float64, ts DateTime('UTC'), v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts) SET ts = max(ts) + INTERVAL 100 YEAR, k = max(v);

SYSTEM STOP MERGES t_ttl_group_by_set_sorting_key;
INSERT INTO t_ttl_group_by_set_sorting_key VALUES (1.0, '2000-06-09 10:00:00', 96827);
INSERT INTO t_ttl_group_by_set_sorting_key VALUES (1.0, '2000-06-10 10:00:00', 41302);
SYSTEM START MERGES t_ttl_group_by_set_sorting_key;
OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;

SELECT '-- The second group gets the key of the first one';
SELECT * FROM t_ttl_group_by_set_sorting_key ORDER BY ALL;

DROP TABLE t_ttl_group_by_set_sorting_key;

CREATE TABLE t_ttl_group_by_set_sorting_key (id String, ts DateTime('UTC'), value String)
ENGINE = MergeTree ORDER BY (id, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY id, toStartOfDay(ts) SET ts = max(ts) + INTERVAL 100 YEAR, id = max(value);

SYSTEM STOP MERGES t_ttl_group_by_set_sorting_key;
INSERT INTO t_ttl_group_by_set_sorting_key VALUES ('pepe', '2000-06-09 10:00:00', 'zzz');
INSERT INTO t_ttl_group_by_set_sorting_key VALUES ('pepe', '2000-06-10 10:00:00', 'aaa');
INSERT INTO t_ttl_group_by_set_sorting_key VALUES ('pepe', '2000-06-11 10:00:00', 'mmm'), ('pepe', '2050-01-01 00:00:00', 'not expired');
SYSTEM START MERGES t_ttl_group_by_set_sorting_key;
OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;

SELECT '-- Aggregated and non-expired rows after the first group get its key';
SELECT * FROM t_ttl_group_by_set_sorting_key ORDER BY ALL;

DROP TABLE t_ttl_group_by_set_sorting_key;
