SET input_format_json_empty_as_default = 1;

-- Simple types with yield_strings formats (JSONStringsEachRow, JSONCompactStringsEachRow)
-- { echoOn }
SELECT x FROM format(JSONStringsEachRow, 'x Date', '{"x":""}');
SELECT x FROM format(JSONStringsEachRow, 'x Date32', '{"x":""}');
SELECT toTimeZone(x, 'UTC') FROM format(JSONStringsEachRow, 'x DateTime', '{"x":""}');
SELECT toTimeZone(x, 'UTC') FROM format(JSONStringsEachRow, 'x DateTime64', '{"x":""}');
SELECT x FROM format(JSONStringsEachRow, 'x IPv4', '{"x":""}');
SELECT x FROM format(JSONStringsEachRow, 'x IPv6', '{"x":""}');
SELECT x FROM format(JSONStringsEachRow, 'x UUID', '{"x":""}');

-- JSONCompactStringsEachRow
SELECT x FROM format(JSONCompactStringsEachRow, 'x Date', '[""]');
SELECT x FROM format(JSONCompactStringsEachRow, 'x IPv6', '[""]');
SELECT x FROM format(JSONCompactStringsEachRow, 'x UUID', '[""]');

-- Nullable
SELECT x FROM format(JSONStringsEachRow, 'x Nullable(IPv6)', '{"x":""}');

-- Multiple columns with some empty
SELECT x, y FROM format(JSONStringsEachRow, 'x Date, y String', '{"x":"", "y":"hello"}');
SELECT x, y FROM format(JSONStringsEachRow, 'x UUID, y UInt32', '{"x":"", "y":"42"}');
