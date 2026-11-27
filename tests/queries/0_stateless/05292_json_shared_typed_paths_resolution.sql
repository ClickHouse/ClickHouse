-- Subcolumn resolution of SHARED typed paths: every form resolves as on the type without SHARED, a
-- typed path under a typed path is the declared one wherever the JSON sits, deeper chains, the reader
-- keys, and bracket access.

SELECT '-- the resolution list, side by side with the plain type';
DROP TABLE IF EXISTS t_shared;
DROP TABLE IF EXISTS t_shared_compact;
DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_shared (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE t_shared_compact (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, write_marks_for_substreams_in_compact_parts = 1;
CREATE TABLE t_plain (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
INSERT INTO t_shared SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), if(number % 2 = 0, concat(',"d":', toString(number)), ''), '}') FROM numbers(8);
INSERT INTO t_shared_compact SELECT * FROM t_shared;
INSERT INTO t_plain SELECT * FROM t_shared;

SELECT x, json.a, json.a.:UInt64, json.b.size, json.b.size0, json.c.size0, json.d, json.d.:Int64, json.a.foo, json.b.foo, json.^a, json.^c, json.^d FROM t_shared ORDER BY x;
SELECT toTypeName(json.a), toTypeName(json.a.:UInt64), toTypeName(json.b.size), toTypeName(json.b.size0), toTypeName(json.c.size0), toTypeName(json.d), toTypeName(json.d.:Int64), toTypeName(json.a.foo), toTypeName(json.^a) FROM t_shared LIMIT 1;
SELECT 'equal to the plain type',
    (SELECT groupArray((x, json.a, json.a.:UInt64, json.b.size, json.b.size0, json.c.size0, json.d, json.d.:Int64, json.a.foo, json.b.foo, json.^a, json.^c, json.^d, toTypeName(json.a), toTypeName(json.b.size0), toTypeName(json.d), toTypeName(json.^a))) FROM (SELECT * FROM t_shared ORDER BY x))
    = (SELECT groupArray((x, json.a, json.a.:UInt64, json.b.size, json.b.size0, json.c.size0, json.d, json.d.:Int64, json.a.foo, json.b.foo, json.^a, json.^c, json.^d, toTypeName(json.a), toTypeName(json.b.size0), toTypeName(json.d), toTypeName(json.^a))) FROM (SELECT * FROM t_plain ORDER BY x)),
    (SELECT groupArray((x, json.a, json.a.:UInt64, json.b.size, json.b.size0, json.c.size0, json.d, json.d.:Int64, json.a.foo, json.b.foo, json.^a, json.^c, json.^d)) FROM (SELECT * FROM t_shared_compact ORDER BY x))
    = (SELECT groupArray((x, json.a, json.a.:UInt64, json.b.size, json.b.size0, json.c.size0, json.d, json.d.:Int64, json.a.foo, json.b.foo, json.^a, json.^c, json.^d)) FROM (SELECT * FROM t_plain ORDER BY x));
SELECT '-- a mismatching hint on a typed path, the same on both types';
SELECT json.a.:String FROM t_shared; -- { serverError UNKNOWN_IDENTIFIER }
SELECT json.a.:String FROM t_plain; -- { serverError UNKNOWN_IDENTIFIER }
SELECT json.c.:`Array(UInt16)` FROM t_shared; -- { serverError UNKNOWN_IDENTIFIER }
SELECT json.c.:`Array(UInt16)` FROM t_plain; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- a typed path under a typed path resolves to the declared one, whatever is SHARED and wherever the JSON sits';
DROP TABLE IF EXISTS t_amb;
CREATE TABLE t_amb (x UInt64,
    j1 JSON(a JSON, a.b Int64), j2 JSON(a JSON, SHARED a.b Int64), j3 JSON(SHARED a JSON, SHARED a.b Int64),
    j4 JSON(a Array(JSON), SHARED a.b Int64), j5 JSON(a Array(JSON), a.b Int64), j6 JSON(a Tuple(b String), SHARED a.b Int64),
    t Tuple(j JSON(SHARED a JSON, SHARED a.b Int64)), arr Array(JSON(SHARED a JSON, SHARED a.b Int64)), m Map(String, JSON(SHARED a JSON, SHARED a.b Int64))
) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;
INSERT INTO t_amb SELECT number,
    concat('{"a":{"b":', toString(number + 5), ',"c":"x"}}'), concat('{"a":{"b":', toString(number + 5), ',"c":"x"}}'), concat('{"a":{"b":', toString(number + 5), ',"c":"x"}}'),
    concat('{"a":[{"c":"x"}],"a":{"b":', toString(number + 5), '}}'), concat('{"a":[{"c":"x"}],"a":{"b":', toString(number + 5), '}}'), concat('{"a":{"b":', toString(number + 5), '}}'),
    tuple(CAST(concat('{"a":{"b":', toString(number + 5), ',"c":"x"}}'), 'JSON(SHARED a JSON, SHARED a.b Int64)')),
    [CAST(concat('{"a":{"b":', toString(number + 5), ',"c":"x"}}'), 'JSON(SHARED a JSON, SHARED a.b Int64)')],
    map('k', CAST(concat('{"a":{"b":', toString(number + 5), ',"c":"x"}}'), 'JSON(SHARED a JSON, SHARED a.b Int64)'))
FROM numbers(8);
SELECT j1.a.b, j2.a.b, j3.a.b, j4.a.b, j5.a.b, j6.a.b, t.j.a.b, arr.a.b, m.values.a.b FROM t_amb ORDER BY x LIMIT 2;
SELECT toTypeName(j1.a.b), toTypeName(j3.a.b), toTypeName(j4.a.b), toTypeName(j6.a.b), toTypeName(t.j.a.b), toTypeName(arr.a.b), toTypeName(m.values.a.b) FROM t_amb LIMIT 1;
SELECT j3.a, j3.^a, j3.a.c, toTypeName(j3.a.c), j6.a FROM t_amb ORDER BY x LIMIT 2;
SELECT 'in memory', j.a.b, toTypeName(j.a.b) FROM (SELECT CAST('{"a":{"b":5}}', 'JSON(a Tuple(b String), a.b Int64)') AS j);
SELECT 'in memory', j.a.b, toTypeName(j.a.b), j.a FROM (SELECT CAST('{"a":{"b":5}}', 'JSON(a Tuple(b String), SHARED a.b Int64)') AS j);

SELECT '-- deeper chains under a shared path, equal to the plain type';
DROP TABLE IF EXISTS t_deep;
DROP TABLE IF EXISTS t_deep_plain;
CREATE TABLE t_deep (x UInt64, json JSON(SHARED a1 Tuple(b Tuple(c UInt64)), SHARED a2 Array(Tuple(b UInt64)), SHARED a3 Map(String, Tuple(x UInt64)), SHARED a4 Variant(Tuple(b UInt64), String), SHARED a5 Dynamic)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE t_deep_plain (x UInt64, json JSON(a1 Tuple(b Tuple(c UInt64)), a2 Array(Tuple(b UInt64)), a3 Map(String, Tuple(x UInt64)), a4 Variant(Tuple(b UInt64), String), a5 Dynamic)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
INSERT INTO t_deep SELECT number, concat('{"a1":{"b":{"c":', toString(number), '}},"a2":[{"b":', toString(number), '},{"b":', toString(number + 1), '}],"a3":{"k":{"x":', toString(number * 2), '}},"a4":', if(number % 2 = 0, concat('{"b":', toString(number), '}'), concat('"s', toString(number), '"')), ',"a5":', if(number % 2 = 0, concat('"d', toString(number), '"'), concat('[', toString(number), ',', toString(number), ']')), '}') FROM numbers(8);
INSERT INTO t_deep_plain SELECT * FROM t_deep;
SELECT x, json.a1.b.c, json.a1.b, json.a1.b.c.zzz, json.a2.b, json.a2.size0, json.a3.values.x, json.a4.`Tuple(b UInt64)`.b, json.a4.String, json.a5.String, json.a5.`Array(Int64)`.size0 FROM t_deep ORDER BY x;
SELECT toTypeName(json.a1.b.c), toTypeName(json.a1.b), toTypeName(json.a1.b.c.zzz), toTypeName(json.a2.b), toTypeName(json.a2.size0), toTypeName(json.a3.values.x), toTypeName(json.a4.`Tuple(b UInt64)`.b), toTypeName(json.a5.String), toTypeName(json.a5.`Array(Int64)`.size0) FROM t_deep LIMIT 1;
SELECT 'equal to the plain type',
    (SELECT groupArray((x, json.a1.b.c, json.a1.b, json.a1.b.c.zzz, json.a2.b, json.a2.size0, json.a3.values.x, json.a4.`Tuple(b UInt64)`.b, json.a4.String, json.a5.String, json.a5.`Array(Int64)`.size0)) FROM (SELECT * FROM t_deep ORDER BY x))
    = (SELECT groupArray((x, json.a1.b.c, json.a1.b, json.a1.b.c.zzz, json.a2.b, json.a2.size0, json.a3.values.x, json.a4.`Tuple(b UInt64)`.b, json.a4.String, json.a5.String, json.a5.`Array(Int64)`.size0)) FROM (SELECT * FROM t_deep_plain ORDER BY x));

SELECT '-- keys of the readers: a sub-subcolumn named like another path, a path named like the whole-column range key';
DROP TABLE IF EXISTS keys_wide;
DROP TABLE IF EXISTS keys_compact_marks;
DROP TABLE IF EXISTS keys_compact_nomarks;
CREATE TABLE keys_wide (x UInt64, json JSON(SHARED a Tuple(b UInt64), SHARED a.b UInt64, SHARED `^.` UInt64)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE keys_compact_marks (x UInt64, json JSON(SHARED a Tuple(b UInt64), SHARED a.b UInt64, SHARED `^.` UInt64)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, write_marks_for_substreams_in_compact_parts = 1;
CREATE TABLE keys_compact_nomarks (x UInt64, json JSON(SHARED a Tuple(b UInt64), SHARED a.b UInt64, SHARED `^.` UInt64)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, write_marks_for_substreams_in_compact_parts = 0;
INSERT INTO keys_wide SELECT number, concat('{"a":{"b":', toString(number + 100), '},"^.":', toString(number + 200), '}') FROM numbers(8);
INSERT INTO keys_compact_marks SELECT * FROM keys_wide;
INSERT INTO keys_compact_nomarks SELECT * FROM keys_wide;
SELECT json.a.b, json.a.:`Tuple(b UInt64)`.b, json.a, json.`^.`, json FROM keys_wide WHERE x = 3;
SELECT json.a.:`Tuple(b UInt64)`.b, json.a.b FROM keys_wide WHERE x = 3;
SELECT json.`^.`, json FROM keys_wide WHERE x = 3;
SELECT json.a.b, json.a.:`Tuple(b UInt64)`.b, json.a, json.`^.`, json FROM keys_compact_marks WHERE x = 3;
SELECT json.a.:`Tuple(b UInt64)`.b, json.a.b FROM keys_compact_marks WHERE x = 3;
SELECT json.a.b, json.a.:`Tuple(b UInt64)`.b, json.a, json.`^.`, json FROM keys_compact_nomarks WHERE x = 3;
SELECT json.a.:`Tuple(b UInt64)`.b, json.a.b FROM keys_compact_nomarks WHERE x = 3;

SELECT '-- a subcolumn of a path named like another path: the path wins, the subcolumn is reached through the hint';
DROP TABLE IF EXISTS keys_sizes;
CREATE TABLE keys_sizes (x UInt64, json JSON(SHARED a Array(UInt64), SHARED a.size0 String)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO keys_sizes VALUES (0, '{"a":[1,2]}'), (1, '{"a":{"size0":"s"}}');
SELECT json.a.size0, json.a.:`Array(UInt64)`.size0, json.a FROM keys_sizes ORDER BY x;

SELECT '-- bracket access is the combined subcolumn of a SHARED path';
DROP TABLE IF EXISTS br_mixed;
DROP TABLE IF EXISTS br_mixed_compact;
DROP TABLE IF EXISTS br_plain;
DROP TABLE IF EXISTS br_memory;
CREATE TABLE br_mixed (x UInt64, json JSON(SHARED a UInt64, SHARED b String, c Array(UInt8), SHARED j JSON(SHARED p UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE br_mixed_compact (x UInt64, json JSON(SHARED a UInt64, SHARED b String, c Array(UInt8), SHARED j JSON(SHARED p UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4;
CREATE TABLE br_plain (x UInt64, json JSON(a UInt64, b String, c Array(UInt8), j JSON(p UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE br_memory (x UInt64, json JSON(SHARED a UInt64, SHARED b String, c Array(UInt8), SHARED j JSON(SHARED p UInt64))) ENGINE = Memory;
INSERT INTO br_mixed SELECT number, concat('{"a":', toString(number * 10), ',"b":"b', toString(number), '","c":', toString(range(number % 3)), ',"d":', toString(number), ',"j":{"p":', toString(number + 7), ',"q":"q"}}') FROM numbers(8);
INSERT INTO br_mixed_compact SELECT * FROM br_mixed;
INSERT INTO br_plain SELECT * FROM br_mixed;
INSERT INTO br_memory SELECT * FROM br_mixed;
SELECT x, json['a'], json['c'], json['d'], json['j'], (json['a'], json.a), json['a'] + 1 FROM br_mixed ORDER BY x LIMIT 3 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'equal across layouts',
    (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_mixed ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1)
    = (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_plain ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1),
    (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_mixed_compact ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1)
    = (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_plain ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1),
    (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_memory ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1)
    = (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_plain ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1),
    (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), br_mixed) ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1)
    = (SELECT groupArray((x, json['a'], json['b'], json['c'], json['d'], json['j'], json['a'] + 1)) FROM (SELECT * FROM br_plain ORDER BY x) SETTINGS optimize_functions_to_subcolumns = 1);
SELECT json, json['b'] FROM br_mixed WHERE x = 5 SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_shared;
DROP TABLE t_shared_compact;
DROP TABLE t_plain;
DROP TABLE t_amb;
DROP TABLE t_deep;
DROP TABLE t_deep_plain;
DROP TABLE keys_wide;
DROP TABLE keys_compact_marks;
DROP TABLE keys_compact_nomarks;
DROP TABLE keys_sizes;
DROP TABLE br_mixed;
DROP TABLE br_mixed_compact;
DROP TABLE br_plain;
DROP TABLE br_memory;
