DROP TABLE IF EXISTS t_sharded_map_3_vertical;

CREATE TABLE t_sharded_map_3_vertical (id UInt64, m Map(LowCardinality(String), UInt64, 4))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_sharded_map_3_vertical SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);
INSERT INTO t_sharded_map_3_vertical SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);

OPTIMIZE TABLE t_sharded_map_3_vertical FINAL;

SYSTEM FLUSH LOGS;

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sharded_map_3_vertical' AND active ORDER BY column;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m FROM t_sharded_map_3_vertical ORDER BY id;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m, m.shard0, m.shard0.keys, m.shard0.size0 FROM t_sharded_map_3_vertical ORDER BY id;
SELECT arraySort(m.keys), m.shard0 FROM t_sharded_map_3_vertical ORDER BY id;
SELECT m.size0, m.shard0 FROM t_sharded_map_3_vertical ORDER BY id;

WITH (SELECT uuid::String FROM system.tables WHERE database = currentDatabase() AND name = 't_sharded_map_3_vertical') AS uuid
SELECT count() > 0 FROM system.text_log WHERE query_id LIKE uuid || '%' AND message LIKE 'Reading%marks from part%column m.shard%';

DROP TABLE t_sharded_map_3_vertical;
