DROP TABLE IF EXISTS t_sharded_map_3;

CREATE TABLE t_sharded_map_3 (id UInt64, m Map(LowCardinality(String), UInt64, 4))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_sharded_map_3 SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);
INSERT INTO t_sharded_map_3 SELECT number, (arrayMap(x -> 'c_' || x::LowCardinality(String), range(number % 10)), range(number % 10)) FROM numbers(10);

OPTIMIZE TABLE t_sharded_map_3 FINAL;

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sharded_map_3' AND active ORDER BY column;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m FROM t_sharded_map_3 ORDER BY id;
SELECT arraySort(m::Array(Tuple(LowCardinality(String), UInt64)))::Map(LowCardinality(String), UInt64) AS m, m.shard0, m.shard0.keys, m.shard0.size0 FROM t_sharded_map_3 ORDER BY id;
SELECT arraySort(m.keys), m.shard0 FROM t_sharded_map_3 ORDER BY id;
SELECT m.size0, m.shard0 FROM t_sharded_map_3 ORDER BY id;

DROP TABLE IF EXISTS t_sharded_map_3;
