WITH CAST(round(sqrt(number)) % 4 AS Enum('' = 0, 'hello' = 1, 'world' = 2, 'test' = 3)) AS x SELECT topK(10)(x) FROM numbers(1000);
