
DROP TABLE IF EXISTS test1;

SELECT * FROM (
    SELECT abs(m2.aa) as e, 1 as aa FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, 2 as aa) m2 ON m1.x = m2.y
);

CREATE TABLE test1 ( `pt` String, `exposure_uv` Float64 ) ENGINE = Memory;
INSERT INTO test1 VALUES ('2021-01-02', 2),('2021-01-01', 1);

SELECT *
FROM
(
        SELECT  m0.pt
               ,m0.exposure_uv AS exposure_uv
               ,round(m2.exposure_uv,4)
        FROM
        (
                SELECT  pt
                       ,exposure_uv
                FROM test1
        ) m0
        LEFT JOIN
        (
                SELECT  pt
                       ,exposure_uv
                FROM test1
        ) m1
        ON m0.pt = m1.pt
        LEFT JOIN
        (
                SELECT  pt
                        ,exposure_uv
                FROM test1
        ) m2
        ON m0.pt = m2.pt
) c0
ORDER BY exposure_uv
settings join_use_nulls = 1;

DROP TABLE IF EXISTS test1;
