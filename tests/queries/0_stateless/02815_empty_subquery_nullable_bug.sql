SELECT * FROM (
    SELECT (
            SELECT 0 AS x
            FROM (SELECT 1 AS x) t1
            JOIN (SELECT 1 AS x) t2 USING (x)
        ) AS x
    FROM ( SELECT 1 AS x )
) FORMAT Null;

SELECT (x IN (111)) == 1
FROM
(
    SELECT ( SELECT 3 :: Nullable(UInt8) WHERE 0 ) AS x
    FROM ( SELECT 2 AS x )
) FORMAT Null;

SELECT (x IN (111)) == 1
FROM
(
    SELECT ( SELECT 3 :: Nullable(UInt8) WHERE 1 ) AS x
    FROM ( SELECT 2 AS x )
) FORMAT Null;

SELECT (x IN (111)) == 1
FROM
(
    SELECT ( SELECT 3 WHERE 0 ) AS x
    FROM ( SELECT 2 AS x )
) FORMAT Null;
