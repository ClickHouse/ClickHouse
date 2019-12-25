SET send_logs_level = 'none';

DROP DATABASE IF EXISTS test_01037;

CREATE DATABASE test_01037 Engine = Ordinary;

DROP DICTIONARY IF EXISTS test_01037.dict;
DROP TABLE IF EXISTS test_01037.polygons;

CREATE TABLE test_01037.polygons (key Array(Array(Array(Array(Float64)))), name String, value UInt64) Engine = Memory;
INSERT INTO test_01037.polygons VALUES ([[[[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1, -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]], [[[5, 5], [5, 1], [7, 1], [7, 7], [1, 7], [1, 5]]]], 'Click', 42);
INSERT INTO test_01037.polygons VALUES ([[[[5, 5], [5, -5], [-5, -5], [-5, 5]], [[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1, -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]]], 'House', 314159);

CREATE DICTIONARY test_01037.dict
(
  key Array(Array(Array(Array(Float64)))),
  name String DEFAULT 'Default',
  value UInt64 DEFAULT 101
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'polygons' PASSWORD '' DB 'test_01037'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(POLYGON());

DROP TABLE IF EXISTS test_01037.points;

CREATE TABLE test_01037.points (x Float64, y Float64) ENGINE = Memory;
INSERT INTO test_01037.points VALUES (0.0, 0.0), (3.0, 3.0), (5.0, 6.0), (-100.0, -42.0), (5.0, 5.0);

select 'dictGet', 'test_01037.dict' as dict_name, tuple(x, y) as key,
       dictGet(dict_name, 'name', key),
       dictGet(dict_name, 'value', key) from test_01037.points;

DROP DICTIONARY test_01037.dict;
DROP TABLE test_01037.polygons;
DROP TABLE test_01037.points;
DROP DATABASE test_01037;