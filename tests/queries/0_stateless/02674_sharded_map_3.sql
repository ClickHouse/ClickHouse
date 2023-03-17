-- Tags: no-random-settings, no-parallel

DROP TABLE IF EXISTS t_sharded_map_3;

CREATE TABLE t_sharded_map_3 (id UInt64, m Map(LowCardinality(String), UInt64, 4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_sharded_map_3 SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);
INSERT INTO t_sharded_map_3 SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);

OPTIMIZE TABLE t_sharded_map_3 FINAL;

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sharded_map_3' AND active ORDER BY column;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m FROM t_sharded_map_3 ORDER BY id;

DROP TABLE IF EXISTS t_sharded_map_3;

CREATE TABLE t_sharded_map_3 (id UInt64, m Map(LowCardinality(String), UInt64, 4))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_sharded_map_3 SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);
INSERT INTO t_sharded_map_3 SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);

OPTIMIZE TABLE t_sharded_map_3 FINAL;

SYSTEM FLUSH LOGS;

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sharded_map_3' AND active ORDER BY column;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m FROM t_sharded_map_3 ORDER BY id;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m, m.shard0 FROM t_sharded_map_3 ORDER BY id;

WITH (SELECT uuid::String FROM system.tables WHERE database = currentDatabase() AND name = 't_sharded_map_3') AS uuid
SELECT count() > 0 FROM system.text_log WHERE query_id LIKE uuid || '%' AND message LIKE 'Reading%marks from part%column m.shard%';

DROP TABLE t_sharded_map_3;
