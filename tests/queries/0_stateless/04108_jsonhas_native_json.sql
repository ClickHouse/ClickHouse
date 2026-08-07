-- Regression for https://github.com/ClickHouse/ClickHouse/issues/102911
-- JSONHas on native JSON must return UInt8 0/1, not the extracted value
-- cast to UInt8.

SELECT '-- constant native JSON';
SELECT JSONHas(CAST('{"key11":123}', 'JSON'), 'key11');
SELECT JSONHas(CAST('{"key11":123}', 'JSON'), 'missing');
SELECT JSONHas(CAST('{"a":{"b":1}}', 'JSON'), 'a', 'b');
SELECT JSONHas(CAST('{"a":{"b":1}}', 'JSON'), 'a', 'c');
SELECT toTypeName(JSONHas(CAST('{"k":1}', 'JSON'), 'k'));

SELECT '-- JSONHas on native and String JSON must agree on presence';
SELECT JSONHas('{"k":1}', 'k'), JSONHas(CAST('{"k":1}', 'JSON'), 'k');
SELECT JSONHas('{"k":1}', 'missing'), JSONHas(CAST('{"k":1}', 'JSON'), 'missing');

SELECT '-- native JSON column';
DROP TABLE IF EXISTS t_jsonhas_native;
CREATE TABLE t_jsonhas_native (id UInt64, j JSON) ENGINE = Memory;
INSERT INTO t_jsonhas_native VALUES (1, '{"a":{"b":1}, "s":"x"}'), (2, '{"a":{"c":2}}'), (3, '{}');
SELECT id, JSONHas(j, 'a'), JSONHas(j, 'a', 'b'), JSONHas(j, 's'), JSONHas(j, 'missing') FROM t_jsonhas_native ORDER BY id;
DROP TABLE t_jsonhas_native;
