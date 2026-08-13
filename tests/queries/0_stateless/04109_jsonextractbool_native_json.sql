-- Regression for https://github.com/ClickHouse/ClickHouse/issues/104373
-- JSONExtractBool on native JSON must return UInt8 0/1 with boolean
-- semantics, not the extracted value cast to UInt8.

SELECT '-- constant native JSON: boolean semantics on numeric values';
SELECT JSONExtractBool(CAST('{"sold":true}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":false}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":1}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":15}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":-1}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":0}', 'JSON'), 'sold');
SELECT toTypeName(JSONExtractBool(CAST('{"k":1}', 'JSON'), 'k'));

SELECT '-- float and string values use Bool cast semantics';
SELECT JSONExtractBool(CAST('{"sold":-3.14159265}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":"foobar"}', 'JSON'), 'sold');
SELECT JSONExtractBool(CAST('{"sold":"true"}', 'JSON'), 'sold');

SELECT '-- missing path returns 0';
SELECT JSONExtractBool(CAST('{"sold":1}', 'JSON'), 'missing');

SELECT '-- JSONExtractBool on native and String JSON must agree';
SELECT JSONExtractBool('{"k":15}', 'k'), JSONExtractBool(CAST('{"k":15}', 'JSON'), 'k');
SELECT JSONExtractBool('{"k":-1}', 'k'), JSONExtractBool(CAST('{"k":-1}', 'JSON'), 'k');
SELECT JSONExtractBool('{"k":0}', 'k'), JSONExtractBool(CAST('{"k":0}', 'JSON'), 'k');
SELECT JSONExtractBool('{"k":"x"}', 'k'), JSONExtractBool(CAST('{"k":"x"}', 'JSON'), 'k');

SELECT '-- native JSON column';
DROP TABLE IF EXISTS t_jsonextractbool_native;
CREATE TABLE t_jsonextractbool_native (id UInt64, j JSON) ENGINE = Memory;
INSERT INTO t_jsonextractbool_native VALUES (1, '{"v":true}'), (2, '{"v":15}'), (3, '{"v":0}'), (4, '{"v":"x"}'), (5, '{}');
SELECT id, JSONExtractBool(j, 'v') FROM t_jsonextractbool_native ORDER BY id;
DROP TABLE t_jsonextractbool_native;
