-- Tags: no-parallel

DROP TABLE IF EXISTS t_sharded_map_1;

CREATE TABLE t_sharded_map_1 (id UInt64, m Map(String, UInt64, 4))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_sharded_map_1 SELECT number, (arrayMap(x -> 'c_' || x::String, range(number % 10)), range(number % 10)) FROM numbers(10);

DROP FUNCTION IF EXISTS mapToArray_02674;
DROP FUNCTION IF EXISTS mapSort_02674;
DROP FUNCTION IF EXISTS mapConcat_02674;

CREATE FUNCTION mapToArray_02674 AS (m) -> m::Array(Tuple(String, UInt64));
CREATE FUNCTION mapSort_02674 AS (m) -> arraySort(mapToArray_02674(m))::Map(String, UInt64);
CREATE FUNCTION mapConcat_02674 AS (m1, m2, m3, m4) -> arrayConcat(mapToArray_02674(m1), mapToArray_02674(m2), mapToArray_02674(m3), mapToArray_02674(m4))::Map(String, UInt64);

SELECT id, mapSort_02674(m) FROM t_sharded_map_1 ORDER BY id;
SELECT id, mapSort_02674(mapConcat_02674(m.shard0, m.shard1, m.shard2, m.shard3)) FROM t_sharded_map_1 ORDER BY id;

DROP TABLE t_sharded_map_1;
