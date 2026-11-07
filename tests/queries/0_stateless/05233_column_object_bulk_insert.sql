CREATE TABLE object_bulk
(
    typed JSON(n UInt64, s String),
    dynamic JSON(max_dynamic_types=1),
    shared JSON(max_dynamic_paths=0),
    mixed JSON(max_dynamic_paths=1, n UInt64)
) ENGINE = Memory;

INSERT INTO object_bulk VALUES
    ('{"n":42,"s":"value"}', '{"n":42,"s":"value"}', '{"n":42,"s":"value"}', '{"n":42,"s":"value","t":[1,2]}');

SELECT count(), uniqExact(toJSONString(typed)), uniqExact(toJSONString(dynamic)),
       uniqExact(toJSONString(shared)), uniqExact(toJSONString(mixed)),
       any(toJSONString(typed)), any(toJSONString(dynamic)),
       any(toJSONString(shared)), any(toJSONString(mixed))
FROM numbers(19) AS l ANY LEFT JOIN object_bulk AS r ON 1
SETTINGS max_block_size=3, query_plan_join_swap_table=0;

SELECT toJSONString(arrayFill(x -> 0, a)), toJSONString(arrayReverseFill(x -> 0, a))
FROM
(
    SELECT arrayMap(x -> x::JSON, ['{"a":1,"b":"first"}', '{}', '{"a":null}', '{"a":"last","c":[1,2]}']) AS a
);

SELECT toJSONString(arrayFill(x -> 0, a)), toJSONString(arrayReverseFill(x -> 0, a))
FROM
(
    SELECT arrayMap(x -> x::JSON(max_dynamic_paths=0), ['{"a":1}', '{}', '{"b":"last","c":2}']) AS a
);

SELECT toJSONString(arrayFill(x -> 0, a)), toJSONString(arrayReverseFill(x -> 0, a))
FROM
(
    SELECT arrayMap(x -> x::JSON(n UInt64, s String), ['{"n":1,"s":"first"}', '{}', '{"n":3,"s":"last"}']) AS a
);

SELECT toJSONString(arrayFill(x -> 0, []::Array(JSON))),
       toJSONString(arrayReverseFill(x -> 0, []::Array(JSON)));

SELECT toJSONString(arrayFill(x -> 0, ['{}'::JSON])),
       toJSONString(arrayReverseFill(x -> 0, ['{}'::JSON]));

DROP TABLE object_bulk;
