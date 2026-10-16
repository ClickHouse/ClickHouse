-- `k IN (subquery)` on a MergeTree key column lets the primary key index prune with the subquery
-- result. To do that it has to move the set from the subquery's element type into the key column
-- type, and `canBeSafelyCast` (src/DataTypes/Utils.cpp) answers whether that is lossless. On `true`
-- KeyCondition casts the set with a plain cast; on `false` it falls back to an accurate cast that
-- rejects the pair when no such cast exists. Both answers are user visible: a wrong `true` loses
-- rows that `IN` must return, so the statements below assert the matched row, and the statements
-- over a type pair without an accurate cast assert the error code name instead.

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_str VALUES ('42'), ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('1.1.1.1'), ('{"a":1}'), ('z');

DROP TABLE IF EXISTS t_i64;
CREATE TABLE t_i64 (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_i64 VALUES (1), (7);

DROP TABLE IF EXISTS t_f64;
CREATE TABLE t_f64 (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_f64 VALUES (1), (7);

DROP TABLE IF EXISTS t_d32_2;
CREATE TABLE t_d32_2 (k Decimal32(2)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_d32_2 VALUES (1), (7);

DROP TABLE IF EXISTS t_d64_4;
CREATE TABLE t_d64_4 (k Decimal64(4)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_d64_4 VALUES (1), (7);

DROP TABLE IF EXISTS t_map_i64;
CREATE TABLE t_map_i64 (k Map(Int64, String)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_map_i64 VALUES (map(1, 'a')), (map(9, 'z'));

DROP TABLE IF EXISTS t_arr_i64_str;
CREATE TABLE t_arr_i64_str (k Array(Tuple(Int64, String))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_arr_i64_str VALUES ([(1, 'a')]), ([(9, 'z')]);

DROP TABLE IF EXISTS t_arr_str_i64;
CREATE TABLE t_arr_str_i64 (k Array(Tuple(String, Int64))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_arr_str_i64 VALUES ([('a', 1)]), ([('z', 9)]);

DROP TABLE IF EXISTS t_arr_str_str;
CREATE TABLE t_arr_str_str (k Array(Tuple(String, String))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_arr_str_str VALUES ([('a', '1')]), ([('z', '9')]);

-- A narrower integer or float widens into the key type, and anything renders into String.
SELECT 'Int32 -> String', k FROM t_str WHERE k IN (SELECT CAST(42, 'Int32')) ORDER BY k;
SELECT 'BFloat16 -> Float64', k FROM t_f64 WHERE k IN (SELECT CAST(1, 'BFloat16')) ORDER BY k;
SELECT 'BFloat16 -> Int64', k FROM t_i64 WHERE k IN (SELECT CAST(arrayJoin([1, 7.5]), 'BFloat16')) ORDER BY k;
SELECT 'Float32 -> Float64', k FROM t_f64 WHERE k IN (SELECT CAST(1, 'Float32')) ORDER BY k;
SELECT 'Float32 -> Int64', k FROM t_i64 WHERE k IN (SELECT CAST(arrayJoin([1, 7.5]), 'Float32')) ORDER BY k;

-- A Decimal needs both a precision and a scale that the key type can hold. A plain cast of
-- 100000000.00 into Decimal32(2) throws DECIMAL_OVERFLOW, the accurate fallback drops it from the set.
SELECT 'Decimal128(2) -> Decimal32(2)', k FROM t_d32_2 WHERE k IN (SELECT CAST(arrayJoin([1, 100000000]), 'Decimal128(2)')) ORDER BY k;
SELECT 'Decimal32(2) -> Decimal64(4)', k FROM t_d64_4 WHERE k IN (SELECT CAST(1, 'Decimal32(2)')) ORDER BY k;

-- UUID and IPv4 are safe into their own width and into String, and have no accurate cast to Int64.
SELECT 'UUID -> String', k FROM t_str WHERE k IN (SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID')) ORDER BY k;
SELECT 'IPv4 -> String', k FROM t_str WHERE k IN (SELECT CAST('1.1.1.1', 'IPv4')) ORDER BY k;
SELECT k FROM t_i64 WHERE k IN (SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID')); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM t_i64 WHERE k IN (SELECT CAST('1.1.1.1', 'IPv4')); -- { serverError NOT_IMPLEMENTED }

-- A Map is safe into a Map and into the Array(Tuple(key, value)) it is stored as, element by
-- element, so String keys reach an Int64 key column only through the accurate cast.
SELECT 'Map -> Map(Int64, String)', k FROM t_map_i64 WHERE k IN (SELECT CAST(map('1', 'a'), 'Map(String, String)')) ORDER BY k;
SELECT 'Map -> Array(Tuple(Int64, String))', k FROM t_arr_i64_str WHERE k IN (SELECT CAST(map('1', 'a'), 'Map(String, String)')) ORDER BY k;
SELECT 'Map -> Array(Tuple(String, Int64))', k FROM t_arr_str_i64 WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, String)')) ORDER BY k;
SELECT 'Map -> Array(Tuple(String, String))', k FROM t_arr_str_str WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, String)')) ORDER BY k;
SELECT k FROM t_f64 WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, String)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- JSON renders into String and has no accurate cast to Int64.
SELECT 'JSON -> String', k FROM t_str WHERE k IN (SELECT CAST('{"a":1}', 'JSON')) ORDER BY k;
SELECT k FROM t_i64 WHERE k IN (SELECT CAST('{"a":1}', 'JSON')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- What the primary key does with each answer, one row per granule. An `Array` key has no accurate
-- fallback, so a `true` prunes and a `false` leaves `Condition: true`. Into `Int64` both float types
-- answer `false`, and the fallback drops 7.5 from the set where a plain cast would keep it as 7.
SELECT extract(explain, 'Condition: .*|Granules: \\d+/\\d+') AS line FROM (EXPLAIN indexes = 1
    SELECT k FROM t_f64 WHERE k IN (SELECT CAST(1, 'Float32'))) WHERE line != '';
SELECT extract(explain, 'Condition: .*|Granules: \\d+/\\d+') AS line FROM (EXPLAIN indexes = 1
    SELECT k FROM t_arr_str_str WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, String)'))) WHERE line != '';
SELECT extract(explain, 'Condition: .*|Granules: \\d+/\\d+') AS line FROM (EXPLAIN indexes = 1
    SELECT k FROM t_arr_i64_str WHERE k IN (SELECT CAST(map('1', 'a'), 'Map(String, String)'))) WHERE line != '';
SELECT extract(explain, 'Condition: .*|Granules: \\d+/\\d+') AS line FROM (EXPLAIN indexes = 1
    SELECT k FROM t_i64 WHERE k IN (SELECT CAST(arrayJoin([1, 7.5]), 'Float32'))) WHERE line != '';
SELECT extract(explain, 'Condition: .*|Granules: \\d+/\\d+') AS line FROM (EXPLAIN indexes = 1
    SELECT k FROM t_i64 WHERE k IN (SELECT CAST(arrayJoin([1, 7.5]), 'BFloat16'))) WHERE line != '';

DROP TABLE t_str;
DROP TABLE t_i64;
DROP TABLE t_f64;
DROP TABLE t_d32_2;
DROP TABLE t_d64_4;
DROP TABLE t_map_i64;
DROP TABLE t_arr_i64_str;
DROP TABLE t_arr_str_i64;
DROP TABLE t_arr_str_str;
