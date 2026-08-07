-- Tags: shard

SELECT avg(arrayJoin([NULL]));
SELECT avg(arrayJoin([NULL, 1]));
SELECT avg(arrayJoin([NULL, 1, 2]));

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(NULL AS Nullable(UInt8)) AS x, CAST(1 AS Nullable(UInt8)) AS y
    UNION ALL
    SELECT CAST(2 AS Nullable(UInt8)) AS x, CAST(NULL AS Nullable(UInt8)) AS y
);

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(1 AS Nullable(UInt8)) AS x, CAST(0 AS Nullable(UInt8)) AS y
    UNION ALL
    SELECT CAST(NULL AS Nullable(UInt8)) AS x, CAST(1 AS Nullable(UInt8)) AS y
    UNION ALL
    SELECT CAST(2 AS Nullable(UInt8)) AS x, CAST(NULL AS Nullable(UInt8)) AS y
    UNION ALL
    SELECT CAST(number AS Nullable(UInt8)) AS x, CAST(number AS Nullable(UInt8)) AS y FROM system.numbers LIMIT 10
);

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(NULL AS Nullable(UInt8)) AS x, 1 AS y
    UNION ALL
    SELECT CAST(2 AS Nullable(UInt8)) AS x, 1 AS y
);

SELECT quantileExactWeighted(0.5)(x, y) FROM
(
    SELECT CAST(NULL AS Nullable(UInt8)) AS x, 1 AS y
);

SELECT
    sum(1 + CAST(dummy AS Nullable(UInt8))) AS res1, toTypeName(res1) AS t1,
    sum(1 + nullIf(dummy, 0)) AS res2, toTypeName(res2) AS t2
FROM remote('127.0.0.{2,3}', system.one);

SELECT CAST(NULL AS Nullable(UInt64)) FROM system.numbers LIMIT 2
