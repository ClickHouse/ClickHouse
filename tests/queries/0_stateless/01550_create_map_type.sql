set allow_experimental_map_type = 1;

-- String type
DROP TABLE IF EXISTS table_map;
CREATE TABLE table_map (a Map(String)) engine = Memory;
INSERT INTO table_map VALUES ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
SELECT a['name'] FROM table_map;
DROP TABLE IF EXISTS table_map;

-- Integer type
DROP TABLE IF EXISTS table_map;
CREATE TABLE table_map (a Map(UInt64)) engine = MergeTree() ORDER BY a;
INSERT INTO table_map SELECT mapBuild('key1', number, 'key2', number * 2) FROM numbers(1111, 3);
SELECT a['key1'], a['key2'] FROM table_map;
DROP TABLE IF EXISTS table_map;

-- MergeTree Engine
DROP TABLE IF EXISTS table_map;
CREATE TABLE table_map (a Map(String), b String) engine = MergeTree() order by a;
INSERT INTO table_map VALUES ({'name':'zhangsan', 'gender':'male'}, 'name'), ({'name':'lisi', 'gender':'female'}, 'gender');
SELECT a[b] FROM table_map;
SELECT b FROM table_map WHERE a = mapBuild('name', 'lisi', 'gender', 'female');
DROP TABLE IF EXISTS table_map;

CREATE TABLE table_map (n UInt32, m Map(Int))
ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0;

-- coversion from Tuple(Array(K), Array(V))
SELECT number, CAST((arrayMap(x -> toString(x), range(number % 10 + 1)), range(number % 10 + 1)), 'Map(Int)') FROM numbers(10);

-- coversion from Array(Tuple(K, V))
SELECT number, CAST(arrayMap(x -> (toString(x), x), range(number % 10 + 1)), 'Map(Int)') FROM numbers(10);

-- FIXME(map): inserting converted map doesn't work!
-- TRUNCATE TABLE table_map;
-- INSERT INTO table_map SELECT number, (arrayMap(x -> toString(x), range(number % 10 + 1)), range(number % 10 + 1)) FROM numbers(10);
-- SELECT * FROM table_map;

-- FIXME(map): inserting converted map doesn't work!
-- TRUNCATE TABLE table_map;
-- INSERT INTO table_map SELECT number, arrayMap(x -> (toString(x), x), range(number % 10 + 1)) FROM numbers(10);
-- SELECT * FROM table_map;

DROP TABLE IF EXISTS table_map;
