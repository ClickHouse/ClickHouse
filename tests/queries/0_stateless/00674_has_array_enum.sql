SELECT has([x], 10) FROM (SELECT CAST(10 AS Enum8('hello' = 1, 'world' = 2, 'abc' = 10)) AS x);
