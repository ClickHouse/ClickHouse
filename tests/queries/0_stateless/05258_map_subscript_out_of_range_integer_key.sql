-- { echo }

-- An integer subscript matches only the key equal to it: one the key type cannot hold matches no key, as in `mapContains`.

DROP TABLE IF EXISTS t_map_basic;
DROP TABLE IF EXISTS t_map_buckets;
DROP TABLE IF EXISTS t_map_text;

CREATE TABLE t_map_basic (id UInt8, m Map(UInt8, String)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO t_map_basic VALUES (1, map(44, 'v44'));

CREATE TABLE t_map_buckets (id UInt8, m Map(UInt8, String)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 4, map_buckets_min_avg_size = 0;
INSERT INTO t_map_buckets VALUES (1, map(40, 'v40', 44, 'v44', 48, 'v48'));

CREATE TABLE t_map_text (id UInt8, b Map(Bool, String), sb Map(SimpleAggregateFunction(anyLast, Bool), String),
    e Map(Enum8('a' = 44, 'b' = 45), String), dt Map(DateTime('Europe/Amsterdam'), String)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 4, map_buckets_min_avg_size = 0;
INSERT INTO t_map_text SELECT 1, map(true, 'vt', false, 'vf'), map(true, 'vt'), map('a', 'va'),
    map(toDateTime(1729989000, 'Europe/Amsterdam'), 'first', toDateTime(1729992600, 'Europe/Amsterdam'), 'second');

SELECT m[300], m[-212], m[44], mapContains(m, 300), mapContains(m, -212) FROM t_map_basic SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m[300], m[-212], m[44] FROM t_map_basic SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m[300], m[-212], m[44] FROM t_map_buckets SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m[300], m[-212], m[44] FROM t_map_buckets SETTINGS optimize_functions_to_subcolumns = 1;
SELECT arrayElementOrNull(m, 300), arrayElementOrNull(m, 44) FROM t_map_basic;
SELECT count() FROM t_map_buckets WHERE m[300] = 'v44';
SELECT k, m[k] FROM t_map_basic ARRAY JOIN [-212, 44, 300]::Array(Int16) AS k ORDER BY k;
SELECT m[materialize(toUInt64(4294967340))] FROM t_map_basic;
SELECT k, map(toUInt8(44), 'x')[k] FROM (SELECT arrayJoin([-212, 44, 300])::Int16 AS k) ORDER BY k;

SELECT count() FROM (EXPLAIN QUERY TREE SELECT m[44] FROM t_map_buckets SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain LIKE '%column_name: m.key_44%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT m[300] FROM t_map_buckets SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain LIKE '%column_name: m.key_%';

SELECT b[2], b[1], b[0], sb[2], sb[1] FROM t_map_text SETTINGS optimize_functions_to_subcolumns = 1;
SELECT e[-1], e[toInt8(46)], e[toInt8(44)] FROM t_map_text SETTINGS optimize_functions_to_subcolumns = 1;
SELECT dt[1729989000], dt[1729992600] FROM t_map_text SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT b[1], sb[1], e[toInt8(44)], dt[1729989000], dt[1729992600] FROM t_map_text SETTINGS optimize_functions_to_subcolumns = 1)
WHERE explain LIKE '%column_name: b.key_true%' OR explain LIKE '%column_name: sb.key_true%' OR explain LIKE '%column_name: e.key_a%' OR explain LIKE '%column_name: dt.key_%';

SELECT map(toUInt64(18446744073709551615), 'x')[-1], map(toInt64(-1), 'x')[18446744073709551615], map(toInt8(-56), 'x')[200], map(toUInt8(255), 'x')[toInt8(-1)];
SELECT map(false, 'x')[256], map(CAST('a' AS Enum8('a' = 44)), 'x')[300], map(toDate(4464), 'x')[70000], map(toLowCardinality(toUInt8(44)), 'x')[300];
SELECT map(toUInt8(44), 'x')[toNullable(300)], map(toUInt8(44), 'x')[toLowCardinality(300)];
SELECT map(toUInt8(44), 'x')[toInt64(44)], map(toInt8(-1), 'x')[toInt32(-1)], map(toInt128(-1), 'x')[-1], map(toUInt8(44), 'x')[toUInt8(300)];

DROP TABLE t_map_basic;
DROP TABLE t_map_buckets;
DROP TABLE t_map_text;
