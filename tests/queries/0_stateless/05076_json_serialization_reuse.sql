-- Tags: no-fasttest
-- Requires `RapidJSON`, which is not included in the Fast test build.

CREATE TABLE json_serialization_reuse
(
    a JSON(x UInt64),
    b JSON(s String),
    c Array(JSON(x UInt64)),
    d Tuple(j JSON(x UInt64))
) ENGINE = Memory;

SET allow_simdjson = 0;
INSERT INTO json_serialization_reuse FORMAT JSONEachRow
{"a":{"x":1},"b":{"s":"first"},"c":[{"x":2}],"d":{"j":{"x":3}}}
{"a":{"x":5},"b":{"s":"second"},"c":[{"x":6}],"d":{"j":{"x":7}}};

SELECT a.x, b.s, c.x, d.j.x FROM json_serialization_reuse ORDER BY a.x;

SET allow_simdjson = 1;
INSERT INTO json_serialization_reuse FORMAT JSONEachRow
{"a":{"x":9},"b":{"s":"third"},"c":[{"x":10}],"d":{"j":{"x":11}}};

SELECT a.x, b.s, c.x, d.j.x FROM json_serialization_reuse ORDER BY a.x;

SELECT sum(getSubcolumn(j, 'x')), sum(length(getSubcolumn(j, 'nested'))) FROM
(
    SELECT concat('{"x":', toString(number), ',"nested":[{"value":1}]}')::JSON(x UInt64, nested Array(JSON)) AS j
    FROM numbers(1000)
) SETTINGS allow_simdjson = 0;
SELECT sum(getSubcolumn(j, 'x')), sum(length(getSubcolumn(j, 'nested'))) FROM
(
    SELECT concat('{"x":', toString(number), ',"nested":[{"value":1}]}')::JSON(x UInt64, nested Array(JSON)) AS j
    FROM numbers(1000)
) SETTINGS allow_simdjson = 1;

SELECT dynamicType(e), getSubcolumn(e, 'JSON.x') FROM (SELECT '{"x":42}'::JSON::Dynamic AS e);

SELECT '{bad json}'::JSON; -- { serverError INCORRECT_DATA }
DROP TABLE json_serialization_reuse;
