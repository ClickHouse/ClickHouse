-- Tags: no-fasttest
-- Reason: needs RapidJSON, which is not enabled in the fast test build.

SELECT JSONRemove('{"a":1,"b":2}', '$.a') FORMAT TSV;
SELECT JSONRemove('{"a":1}', '$.a') FORMAT TSV;
SELECT JSONRemove('[1]', '$[0]') FORMAT TSV;
SELECT JSONRemove('{"a":{"b":1,"c":2},"d":3}', '$.a.b') FORMAT TSV;
SELECT JSONRemove('{"items":[{"secret":1},{"secret":2}]}', '$.items[1].secret') FORMAT TSV;
SELECT JSONRemove('[0,1,2]', '$[0]', '$[1]') FORMAT TSV;
SELECT JSONRemove('{"a":1}', '$.missing') FORMAT TSV;
SELECT JSONRemove('{"a":1}', '$.a.b') FORMAT TSV;
SELECT JSONRemove('{"a.b":1,"c":2}', '$["a.b"]') FORMAT TSV;
SELECT JSONRemove('[0,1,2]', '$[1 to 2]') FORMAT TSV;
SELECT JSON_REMOVE('[0,1,2]', '$[1]') FORMAT TSV;
SELECT JSONRemove('42', '$.a') FORMAT TSV;
SELECT JSONRemove(' { "a" : 1 } ', '$.missing') FORMAT TSV;
SELECT JSONRemove('{"a":1,"a":2,"b":3}', '$.a') FORMAT TSV;
SELECT JSONRemove('{"a":{"x":1,"keep":2},"a":{"x":3,"keep":4}}', '$.a.x') FORMAT TSV;
SELECT JSONRemove('{"a":0,"a":{"x":1}}', '$.a.x') FORMAT TSV;
SELECT JSONRemove('{"a":{"b":{"x":1},"b":{"x":2}},"a":{"b":{"x":3}}}', '$.a.b.x') FORMAT TSV;
SELECT JSONRemove('{"a":[{"x":1},{"x":2}],"a":[{"x":3},{"x":4}]}', '$.a[1].x') FORMAT TSV;
SELECT JSONRemove('{"big":18446744073709551617,"a":1,"exp":1e+308,"text":"18446744073709551617"}', '$.a') FORMAT TSV;
SELECT JSONRemove('{"drop":2,"neg_zero":-0,"decimal":1.00,"exp":1e-2}', '$.drop') FORMAT TSV;
SELECT JSONRemove('{"a":0,"nested":[1.00,{"keep":1e-2,"drop":2}]}', '$.nested[1].drop') FORMAT TSV;
SELECT JSONRemove('{"drop":{"n":18446744073709551617},"keep":18446744073709551618}', '$.drop') FORMAT TSV;
SELECT JSONRemove('[18446744073709551617,18446744073709551618,3]', '$[0]') FORMAT TSV;
SELECT JSONRemove('[{"n":18446744073709551617},18446744073709551618,18446744073709551619]', '$[0]', '$[1]') FORMAT TSV;
SELECT JSONRemove(concat('{"huge":', repeat('9', 400), ',"keep":1}'), '$.huge') FORMAT TSV;
SELECT JSONRemove(concat('{"keep":', repeat('9', 400), ',"drop":1}'), '$.drop') = concat('{"keep":', repeat('9', 400), '}') FORMAT TSV;
SELECT JSONRemove(unhex('7B225C7530303631223A312C226B656570223A225C75303065395C2F227D'), '$.a') FORMAT TSV;
SELECT JSONRemove(unhex('7B225C75303036625C75303036355C7530303739223A22615C5C5C22625C2F63222C2264726F70223A317D'), '$.drop') FORMAT TSV;
SELECT JSONRemove(concat('{"a":', toString(number), '}'), '$.a') FROM numbers(3) FORMAT TSV;
SELECT JSONRemove(data, '$.a')
FROM VALUES('data String', ('{"a":1}'), ('{"a":2}'))
ORDER BY data FORMAT TSV;
SELECT JSONRemove(data, '$.a')
FROM (SELECT toLowCardinality(arrayJoin(['{"a":1}', '{"a":2}'])) AS data)
ORDER BY data FORMAT TSV;
SELECT JSONRemove(CAST('{"a":1}' AS Nullable(String)), '$.a') FORMAT TSV;
SELECT JSONRemove(data, '$.a')
FROM
(
    SELECT arrayJoin([CAST('{"a":1}' AS Nullable(String)), CAST(NULL AS Nullable(String))]) AS data
)
FORMAT TSV;
SELECT JSONRemove('{}', CAST(NULL AS Nullable(String))) FORMAT TSV;
SELECT JSONRemove(unhex('7B2261223A312C225C7530303631223A322C2262223A337D'), '$.a') FORMAT TSV;
WITH concat(repeat('[', 1001), '0', repeat(']', 1001)) AS json
SELECT JSONRemove(json, '$.missing') = json FORMAT TSV;

SELECT JSONRemove('[0,1,2]', '$') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[*]') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[0,2]') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[0 to 2]') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('{', '$.a') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('{"a":1} {"b":2}', '$.x') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[]', concat('$[', toString(number), ']')) FROM numbers(1) FORMAT TSV; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Embedded NUL bytes must not silently truncate the input.
SELECT JSONRemove(concat('{"a":1}', char(0), '{"b":2}'), '$.x') FORMAT TSV; -- { serverError BAD_ARGUMENTS }

-- A constant JSON input must keep a constant result even on an empty block.
SELECT JSONRemove('{"a":1,"b":2}', '$.a') FROM numbers(0) FORMAT TSV;
SELECT JSONRemove('{"a":1,"b":2}', '$.a') FROM numbers(0)
UNION ALL
SELECT JSONRemove('{"a":1,"b":2}', '$.a') FROM numbers(1)
FORMAT TSV;
