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
SELECT JSONRemove('{"big":18446744073709551617,"a":1,"exp":1e+308,"text":"18446744073709551617"}', '$.a') FORMAT TSV;
SELECT JSONRemove('{"drop":2,"neg_zero":-0,"decimal":1.00,"exp":1e-2}', '$.drop') FORMAT TSV;
SELECT JSONRemove('{"a":0,"nested":[1.00,{"keep":1e-2,"drop":2}]}', '$.nested[1].drop') FORMAT TSV;
SELECT JSONRemove('{"drop":{"n":18446744073709551617},"keep":18446744073709551618}', '$.drop') FORMAT TSV;
SELECT JSONRemove('[18446744073709551617,18446744073709551618,3]', '$[0]') FORMAT TSV;
SELECT JSONRemove('[{"n":18446744073709551617},18446744073709551618,18446744073709551619]', '$[0]', '$[1]') FORMAT TSV;
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

SELECT JSONRemove('[0,1,2]', '$') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[*]') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[0,2]') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[0 to 2]') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[0,1,2]', '$[') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('{', '$.a') FORMAT TSV; -- { serverError BAD_ARGUMENTS }
SELECT JSONRemove('[]', concat('$[', toString(number), ']')) FROM numbers(1) FORMAT TSV; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONRemove(concat(repeat('[', 1001), '0', repeat(']', 1001)), '$[0]') FORMAT TSV; -- { serverError TOO_DEEP_RECURSION }
