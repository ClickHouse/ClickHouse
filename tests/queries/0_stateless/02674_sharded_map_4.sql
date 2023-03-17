DROP TABLE IF EXISTS t_sharded_map_4;

CREATE TABLE t_sharded_map_4 (id UInt64, m Map(String, UInt64, 4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_sharded_map_4 SELECT number, (arrayMap(x -> 'c_' || x::String, range(number % 10)), range(number % 10)) FROM numbers(6);

SELECT arraySort(m.keys), arraySort(m.values) FROM t_sharded_map_4 ORDER BY id;
SELECT arraySort(arrayConcat(m.shard0.keys, m.shard1.keys, m.shard2.keys, m.shard3.keys)) FROM t_sharded_map_4;

SELECT m.size0 FROM t_sharded_map_4;
SELECT m.shard0.size0 + m.shard1.size0 + m.shard2.size0 + m.shard3.size0 FROM t_sharded_map_4;

DROP TABLE t_sharded_map_4;

SELECT '*****************';

CREATE TABLE t_sharded_map_4 (id UInt64, m Map(String, Array(Nullable(String)), 4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_sharded_map_4 VALUES (1, map('k1', ['foo', NULL, 'bar'], 'k2', ['qqq']));
INSERT INTO t_sharded_map_4 VALUES (2, map('k3', []));

SELECT arraySort(m.keys), arraySort(m.values) FROM t_sharded_map_4 ORDER BY id;
SELECT m.size0 FROM t_sharded_map_4 ORDER BY id;
SELECT m.values.size1, m.values.null FROM t_sharded_map_4 ORDER BY id;

DROP TABLE t_sharded_map_4;

SELECT '*****************';

CREATE TABLE t_sharded_map_4 (id UInt64, m Array(Map(String, UInt64, 4)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_sharded_map_4 VALUES (1, [map('k1', 1, 'k2', 2), map('k3', 3)]);
INSERT INTO t_sharded_map_4 VALUES (2, []);
INSERT INTO t_sharded_map_4 VALUES (3, [map('k4', 4), map()]);

SELECT arraySort(m.keys), arraySort(m.values) FROM t_sharded_map_4 ORDER BY id;
SELECT m.size0 FROM t_sharded_map_4 ORDER BY id;
SELECT m.size1 FROM t_sharded_map_4 ORDER BY id;
SELECT m.values, m.shard1 FROM t_sharded_map_4 ORDER BY id;

DROP TABLE t_sharded_map_4;
