-- SHARED typed paths of the JSON type: declaration and parser, type operations, DESCRIBE and the system
-- tables, functions over the type, and the limits the SHARED form inherits from the plain one.

SELECT '-- declarations';
DROP TABLE IF EXISTS t_shared;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_mixed;
DROP TABLE IF EXISTS t_params;
DROP TABLE IF EXISTS t_containers;
CREATE TABLE t_shared (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE t_plain (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE t_mixed (x UInt64, json JSON(a UInt64, SHARED b String, SHARED c Array(UInt8))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1;
CREATE TABLE t_params (x UInt64, json JSON(max_dynamic_paths = 2, max_dynamic_types = 4, SHARED a UInt64, SKIP s, SKIP REGEXP '^z.*')) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;
CREATE TABLE t_containers (x UInt64,
    t Tuple(j JSON(SHARED a UInt64, SHARED n JSON(SHARED p String)), s String),
    arr Array(JSON(SHARED c Array(UInt8))),
    m Map(String, JSON(SHARED a UInt64)),
    v Variant(JSON(SHARED a UInt64), String),
    aa Array(Array(JSON(SHARED c Array(UInt8)))),
    ma Map(String, Array(JSON(SHARED a UInt64))),
    n Nested(j JSON(SHARED a UInt64), k UInt8)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;
SHOW CREATE TABLE t_mixed;
SELECT table, name, type FROM system.columns WHERE database = currentDatabase() AND table IN ('t_shared', 't_params') ORDER BY table, name;
SELECT toTypeName(CAST('{}', 'JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))'));
SELECT toTypeName(CAST('{}', 'JSON(max_dynamic_paths = 2, SHARED a.b UInt64, a.c String, SKIP d)'));
SELECT toTypeName([CAST('{}', 'JSON(SHARED a UInt64)')]), toTypeName(tuple([CAST('{}', 'JSON(SHARED a UInt64)')])), toTypeName(map('k', CAST('{}', 'JSON(SHARED a UInt64)')));

INSERT INTO t_shared SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), if(number % 2 = 0, concat(',"d":', toString(number)), ''), '}') FROM numbers(8);
INSERT INTO t_plain SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), if(number % 2 = 0, concat(',"d":', toString(number)), ''), '}') FROM numbers(8);
INSERT INTO t_mixed SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), if(number % 2 = 0, concat(',"d":', toString(number)), ''), '}') FROM numbers(8);
INSERT INTO t_params SELECT number, concat('{"a":', toString(number), ',"s":1,"zz":2,"p1":3,"p2":4,"p3":5}') FROM numbers(4);
INSERT INTO t_containers SELECT number,
    tuple(CAST(concat('{"a":', toString(number), ',"n":{"p":"p', toString(number), '"}}'), 'JSON(SHARED a UInt64, SHARED n JSON(SHARED p String))'), concat('s', toString(number))),
    arrayMap(i -> CAST(concat('{"c":', toString(range(i)), '}'), 'JSON(SHARED c Array(UInt8))'), range(number % 3)),
    map('k', CAST(concat('{"a":', toString(number + 100), '}'), 'JSON(SHARED a UInt64)')),
    if(number % 2 = 0, CAST(concat('{"a":', toString(number + 200), '}'), 'JSON(SHARED a UInt64)')::Variant(JSON(SHARED a UInt64), String), concat('v', toString(number))::Variant(JSON(SHARED a UInt64), String)),
    [arrayMap(i -> CAST(concat('{"c":', toString(range(i)), '}'), 'JSON(SHARED c Array(UInt8))'), range(number % 3))],
    map('k', arrayMap(i -> CAST(concat('{"a":', toString(number + i), '}'), 'JSON(SHARED a UInt64)'), range(number % 3))),
    arrayMap(i -> CAST(concat('{"a":', toString(number * 10 + i), '}'), 'JSON(SHARED a UInt64)'), range(number % 3)),
    arrayMap(i -> toUInt8(i), range(number % 3))
FROM numbers(6);
SELECT count(), sum(json.a), sum(JSONDynamicPaths(json) = ['p1', 'p2']), sum(JSONSharedDataPaths(json) = ['p3']), sum(JSONAllPaths(json) = ['a', 'p1', 'p2', 'p3']) FROM t_params;

SELECT '-- declaration errors';
SELECT toTypeName(CAST('{}', 'JSON(SHARED a UInt64, a String)')); -- { serverError BAD_ARGUMENTS }
SELECT toTypeName(CAST('{}', 'JSON(SHARED a UInt64, SHARED a UInt64)')); -- { serverError BAD_ARGUMENTS }
SELECT toTypeName(CAST('{}', 'JSON(SHARED a.b UInt64, SKIP a)')); -- { serverError BAD_ARGUMENTS }
SELECT toTypeName(CAST('{}', 'JSON(SHARED a.b UInt64, SKIP REGEXP ''a\\..*'')')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (x UInt64, json JSON(SHARED a UInt64)) ENGINE = MergeTree ORDER BY x SETTINGS object_typed_shared_data_serialization_version = 'v2'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_shared MODIFY SETTING object_typed_shared_data_serialization_version = 'v2'; -- { serverError BAD_ARGUMENTS }

SELECT '-- parser: a path named shared or SHARED, keyword case, single- and multi-word types';
SELECT toTypeName(CAST('{}', 'JSON(shared DOUBLE PRECISION)'));
SELECT toTypeName(CAST('{}', 'JSON(SHARED DOUBLE PRECISION)'));
SELECT toTypeName(CAST('{}', 'JSON(shared CHAR VARYING, SHARED b String)'));
SELECT toTypeName(CAST('{}', 'JSON(shared Nullable(Int64))'));
SELECT toTypeName(CAST('{}', 'JSON(SHARED shared UInt64)'));
SELECT toTypeName(CAST('{}', 'JSON(shared.x UInt64)'));
SELECT toTypeName(CAST('{}', 'JSON(SHARED a DOUBLE PRECISION)'));
SELECT toTypeName(CAST('{}', 'JSON(SHARED a.b UInt64, max_dynamic_paths = 2)'));
SELECT toTypeName(CAST('{}', 'JSON(`shared` DOUBLE PRECISION)'));
SELECT toTypeName(CAST('{}', 'JSON(SHARED SKIP UInt64, SKIP shared)'));
SELECT toTypeName(CAST('{}', 'JSON(shared a UInt64)'));
SELECT toTypeName(CAST('{}', 'JSON(Shared a UInt64, `Shared` String)'));
SELECT toTypeName(CAST('{}', 'JSON(SHARED)')); -- { serverError SYNTAX_ERROR }
SELECT toTypeName(CAST('{}', 'JSON(SHARED a)')); -- { serverError UNKNOWN_TYPE }
SELECT toTypeName(CAST('{}', 'JSON(SHARED a UInt64 String)')); -- { serverError SYNTAX_ERROR }
SELECT toTypeName(CAST('{}', 'JSON(SHARED max_dynamic_paths = 2)')); -- { serverError SYNTAX_ERROR }
SELECT toTypeName(CAST('{}', 'JSON(shared = 1)')); -- { serverError UNEXPECTED_AST_STRUCTURE }
SELECT formatQuery('CREATE TABLE f (json JSON(SHARED a UInt64, `shared` String, shared.x UInt64, SHARED b.c Array(UInt8))) ENGINE = Memory');
EXPLAIN AST SELECT CAST('{}', 'JSON(SHARED a UInt64, shared String)');

SELECT '-- type operations';
SELECT toTypeName(if(x % 2 = 0, CAST('{}', 'JSON(SHARED a UInt64)'), CAST('{}', 'JSON(a UInt64)'))) FROM (SELECT 1 AS x);
SELECT toTypeName(if(x % 2 = 0, CAST('{}', 'JSON(SHARED a UInt64)'), CAST('{}', 'JSON(SHARED a UInt64)'))) FROM (SELECT 1 AS x);
SELECT toTypeName([CAST('{}', 'JSON(SHARED a UInt64, SHARED b String)'), CAST('{}', 'JSON(SHARED a UInt64, b String)')]);
SELECT toTypeName([CAST('{}', 'JSON(SHARED a UInt64)'), CAST('{}', 'JSON(SHARED a String)')]);
SELECT toTypeName([CAST('{}', 'JSON(SHARED a UInt64)'), CAST('{}', 'JSON(SHARED a UInt64, max_dynamic_paths = 2)')]);
SELECT toTypeName(json), count() FROM (SELECT json FROM t_shared UNION ALL SELECT json FROM t_plain) GROUP BY 1;
SELECT toTypeName(json), count() FROM (SELECT json FROM t_shared UNION ALL SELECT json FROM t_shared) GROUP BY 1;
DROP TABLE IF EXISTS t_copy;
CREATE TABLE t_copy (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;
INSERT INTO t_copy SELECT * FROM t_plain;
SELECT count(), sum(json.a), sum(length(json.c)), sum(json.d.:Int64) FROM t_copy;
DROP TABLE IF EXISTS t_plain_copy;
CREATE TABLE t_plain_copy (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;
INSERT INTO t_plain_copy SELECT * FROM t_shared;
SELECT count(), sum(json.a), sum(length(json.c)), sum(json.d.:Int64) FROM t_plain_copy;
SELECT toTypeName(CAST(json, 'JSON(a UInt64, b String, c Array(UInt8))')), toTypeName(CAST(json, 'JSON(a UInt64, SHARED b String)')), toTypeName(CAST(json, 'JSON(SHARED a UInt64, max_dynamic_paths = 1)')) FROM t_shared LIMIT 1;
SELECT CAST(json, 'JSON(a UInt64, SHARED b String)'), CAST(json, 'JSON(SHARED a UInt64, max_dynamic_paths = 1)'), JSONDynamicPaths(CAST(json, 'JSON(SHARED a UInt64, max_dynamic_paths = 1)')) FROM t_shared ORDER BY x LIMIT 3;
SELECT CAST(json, 'String') = CAST(CAST(json, 'JSON(a UInt64, b String, c Array(UInt8))'), 'String') FROM t_shared ORDER BY x LIMIT 2;
SELECT JSONExtract('{"a":7,"b":"q"}', 'JSON(SHARED a UInt64)'), toTypeName(JSONExtract('{"a":7}', 'JSON(SHARED a UInt64)'));
SELECT '{"a":8}'::JSON(SHARED a UInt64), defaultValueOfTypeName('JSON(SHARED a UInt64, SHARED c Array(UInt8))');
SELECT dynamicType(d), d.`JSON(SHARED a UInt64)`.a, d FROM (SELECT CAST(CAST('{"a":7,"z":1}', 'JSON(SHARED a UInt64)'), 'Dynamic') AS d);

SELECT '-- DESCRIBE lists the shared paths and their subcolumns like typed paths';
DESCRIBE t_shared SETTINGS describe_include_subcolumns = 1;
DESCRIBE t_mixed SETTINGS describe_include_subcolumns = 1;
DESCRIBE t_plain SETTINGS describe_include_subcolumns = 1;
DESCRIBE t_containers SETTINGS describe_include_subcolumns = 1;
SELECT t.j.a, t.j.n.p, t.s, arr.c.size1, m.values.a, v.`JSON(SHARED a UInt64)`.a, aa.c.size2, ma.values.a, n.k FROM t_containers ORDER BY x;
SELECT '-- system tables';
SELECT table, column, subcolumns.names FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t_shared', 't_mixed', 't_plain') AND column = 'json' AND active ORDER BY table;
SELECT name, value FROM system.merge_tree_settings WHERE name LIKE 'object_typed_shared_data%' ORDER BY name;

SELECT '-- functions over the type';
SELECT toJSONString(json), JSONExtractKeys(toJSONString(json)), empty(json), json = json FROM t_shared ORDER BY x LIMIT 2;
SELECT JSONExtractKeys(toJSONString(b.json)), JSONExtractUInt(toJSONString(a.json), 'a') + JSONExtractUInt(toJSONString(b.json), 'a') FROM t_shared AS a, t_shared AS b WHERE a.x = 1 AND b.x = 2;
SELECT json.a, count() FROM t_shared GROUP BY json.a ORDER BY json.a LIMIT 3;
SELECT arrayJoin(arr).c, tupleElement(t, 'j').a FROM t_containers WHERE x = 2 ORDER BY 1;
SELECT JSONAllPaths(json), JSONDynamicPaths(json), JSONSharedDataPaths(json), JSONAllPathsWithTypes(json) FROM t_shared WHERE x = 2;
SELECT JSONAllPaths(json), JSONDynamicPaths(json), JSONSharedDataPaths(json), JSONAllPathsWithTypes(json) FROM t_plain WHERE x = 2;

SELECT '-- limits inherited from the plain type';
ALTER TABLE t_shared ADD PROJECTION p (SELECT json.a, count() GROUP BY json.a); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_plain ADD PROJECTION p (SELECT json.a, count() GROUP BY json.a); -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t_log (json JSON(SHARED a UInt64)) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_log (json JSON(a UInt64)) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_shared ATTACH PARTITION tuple() FROM t_plain; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT '-- the stream name collision check sees typed paths; a shared path has no file';
CREATE TABLE t_collision (x UInt64, json JSON(a Tuple(b String), a.b Int64)) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS t_collision;
CREATE TABLE t_collision (x UInt64, json JSON(a Tuple(b String), SHARED a.b Int64)) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_collision SELECT number, concat('{"a":{"b":', toString(number + 5), '}}') FROM numbers(3);
SELECT json.a.b, toTypeName(json.a.b), json.a FROM t_collision ORDER BY x;
ALTER TABLE t_collision MODIFY COLUMN json JSON(a Tuple(b String), a.b Int64); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_collision ADD COLUMN json2 JSON(a Tuple(b String), a.b Int64); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_collision ADD COLUMN json2 JSON(a Tuple(b String), SHARED a.b Int64);
SELECT json2.a.b, toTypeName(json2.a.b) FROM t_collision ORDER BY x LIMIT 1;
SELECT '-- a column literally named json.a beside the path a: its file name is escaped, no collision in either form';
DROP TABLE IF EXISTS t_sibling_plain;
DROP TABLE IF EXISTS t_sibling_shared;
CREATE TABLE t_sibling_plain (x UInt64, json JSON(a UInt64), `json.a` UInt64) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_sibling_shared (x UInt64, json JSON(SHARED a UInt64), `json.a` UInt64) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_plain SELECT number, concat('{"a":', toString(number), '}'), number + 100 FROM numbers(3);
INSERT INTO t_sibling_shared SELECT number, concat('{"a":', toString(number), '}'), number + 100 FROM numbers(3);
SELECT json.a, `json.a`, json FROM t_sibling_plain ORDER BY x;
SELECT json.a, `json.a`, json FROM t_sibling_shared ORDER BY x;

DROP TABLE t_shared;
DROP TABLE t_plain;
DROP TABLE t_mixed;
DROP TABLE t_params;
DROP TABLE t_containers;
DROP TABLE t_copy;
DROP TABLE t_plain_copy;
DROP TABLE t_collision;
DROP TABLE t_sibling_plain;
DROP TABLE t_sibling_shared;
