-- Tags: no-fasttest

SELECT cityHash64(*) FROM (SELECT 1 AS x, CAST(x AS Enum8('Hello' = 0, 'World' = 1)) AS y);
SELECT cityHash64(*) FROM (SELECT 1 AS x, x AS y);
