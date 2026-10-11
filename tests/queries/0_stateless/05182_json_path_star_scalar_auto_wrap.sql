-- Tests for RFC 9535 lax-mode auto-wrapping of non-array values under the JSONPath wildcard `[*]`.
-- https://github.com/ClickHouse/ClickHouse/issues/118755

-- { echo }
SELECT JSON_QUERY('{"p":3}', '$.p[*]');
SELECT JSON_QUERY('{"arr":[{"p":[1,2]},{"p":3}]}', '$.arr[*].p[*]');
SELECT JSON_QUERY('{"p":"str"}', '$.p[*]');
SELECT JSON_QUERY('{"p":null}', '$.p[*]');
SELECT JSON_QUERY('{"p":{"x":1}}', '$.p[*]');
SELECT JSON_QUERY('{"p":{"x":1}}', '$.p[*].x');
SELECT JSON_QUERY('{"p":{"q":[1,2]}}', '$.p[*].q[*]');
SELECT JSON_QUERY('{"hello":1}', '$[*]');
SELECT JSON_QUERY('{"p":[1,2]}', '$.p[*]');
SELECT JSON_QUERY('{"p":3}', '$.q[*]');
SELECT JSON_VALUE('{"p":3}', '$.p[*]');
SELECT JSON_VALUE('{"p":"str"}', '$.p[*]');
SELECT JSON_EXISTS('{"p":3}', '$.p[*]');
SELECT JSON_EXISTS('{"p":3}', '$.q[*]');
