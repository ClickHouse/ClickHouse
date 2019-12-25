SET send_logs_level = 'none';

DROP DATABASE IF EXISTS test_01037;

CREATE DATABASE test_01037 Engine = Ordinary;

DROP TABLE IF EXISTS test_01037.polygons;

CREATE TABLE polygons (key Array(Array(Array(Array(Float64)))), name String, u64 UInt64) Engine = Memory;
INSERT INTO polygons VALUES ([[[[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1. -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]], [[[5, 5], [5, 1], [7, 1], [7, 7], [1, 7], [1, 5]]]], 'Click', 42);
INSERT INTO polygons VALUES ([[[[5, 5], [5, -5], [-5, -5], [-5, 5]], [[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1. -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]]], 'House', 314159);

CREATE DICTIONARY test_01037.dict
(
  polygon Array(Array(Array(Array(Float64)))),
  name String DEFAULT '',
  value UInt64 DEFAULT 42
)
PRIMARY KEY polygon
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'polygons' PASSWORD '' DB 'test_01037'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(POLYGON())

select 'dictGet', 'test_01037.dict' as dict_name, tuple(0.0, 0.0) as key,
       dictGet(dict_name, 'name', key),
       dictGet(dict_name, 'u64', key);

DROP DICTIONARY IF EXISTS test_01037.dict;
DROP TABLE IF EXISTS test_01037.polygons;
DROP DATABASE IF EXISTS test_01037;
