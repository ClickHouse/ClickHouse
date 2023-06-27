
DROP TABLE IF EXISTS test1;

SELECT abs(m2.aa) as e, 'aa' as aa FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;
SELECT aa || 'a' as e, 'aa' as aa FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;
SELECT abs(aa) as aa, aa as e FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;
SELECT abs(aa) as aa, m2.aa as ee, aa as e FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;
SELECT abs(m2.aa) as aa, aa as e FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;
SELECT abs(m2.aa) as aa, m2.aa as e FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;
SELECT abs(aa) as aa, m2.aa as e FROM (SELECT 1 as x) m1 JOIN (SELECT 1 as y, -2 as aa) m2 ON m1.x = m2.y;

SELECT aa || 'a' as e, 'aa' as aa FROM (SELECT 1 as x, -2 as aa) m1 JOIN (SELECT 1 as y) m2 ON m1.x = m2.y;
SELECT abs(aa) as aa, aa as e FROM (SELECT 1 as x, -2 as aa) m1 JOIN (SELECT 1 as y) m2 ON m1.x = m2.y;
SELECT abs(aa) as aa, m1.aa as ee, aa as e FROM (SELECT 1 as x, -2 as aa) m1 JOIN (SELECT 1 as y) m2 ON m1.x = m2.y;
SELECT abs(m1.aa) as aa, aa as e FROM (SELECT 1 as x, -2 as aa) m1 JOIN (SELECT 1 as y) m2 ON m1.x = m2.y;
SELECT abs(m1.aa) as aa, m1.aa as e FROM (SELECT 1 as x, -2 as aa) m1 JOIN (SELECT 1 as y) m2 ON m1.x = m2.y;
SELECT abs(aa) as aa, m1.aa as e FROM (SELECT 1 as x, -2 as aa) m1 JOIN (SELECT 1 as y) m2 ON m1.x = m2.y;

CREATE TABLE test1 ( `pt` String, `exposure_uv` Float64 ) ENGINE = TinyLog;
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


SELECT
    pt AS pt,
    exposure_uv AS exposure_uv
FROM (
    SELECT pt FROM test1
) AS m0
FULL OUTER JOIN (
    SELECT pt, exposure_uv FROM test1
) AS m1
ON m0.pt = m1.pt
ORDER BY 1;

SELECT
    pt AS pt,
    round(exposure_uv, 4) AS exposure_uv
FROM (
    SELECT pt FROM test1
) AS m0
FULL OUTER JOIN (
    SELECT pt, exposure_uv FROM test1
) AS m1
ON m0.pt = m1.pt
ORDER BY 1;

DROP TABLE IF EXISTS test1;
