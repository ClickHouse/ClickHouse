SELECT toBool(sin(SUM(number))) AS x
FROM
(
    SELECT 1 AS number
)
GROUP BY number
HAVING 1 AND sin(sum(number))
ORDER BY ALL
SETTINGS enable_optimize_predicate_expression = 0;

SELECT '=====';

SELECT toBool(sin(SUM(number))) AS x
FROM
(
    SELECT 1 AS number
)
GROUP BY number
HAVING 1 AND sin(1)
ORDER BY ALL
SETTINGS enable_optimize_predicate_expression = 0;

SELECT '=====';

SELECT toBool(sin(SUM(number))) AS x
FROM
(
    SELECT 1 AS number
)
GROUP BY number
HAVING x AND sin(sum(number))
ORDER BY ALL
SETTINGS enable_optimize_predicate_expression = 1;

SELECT '=====';

SELECT toBool(sin(SUM(number))) AS x
FROM
(
    SELECT 1 AS number
)
GROUP BY number
HAVING 1 AND sin(sum(number))
ORDER BY ALL
SETTINGS enable_optimize_predicate_expression = 0;

SELECT '=====';

SELECT 1 and sin(1);

SELECT '=====';

SELECT 'enable_analyzer';

SET enable_analyzer = 1;

SELECT toBool(sin(SUM(number))) AS x
FROM
(
    SELECT 1 AS number
)
GROUP BY number
HAVING 1 AND sin(sum(number))
ORDER BY ALL
SETTINGS enable_optimize_predicate_expression = 1;

select '#45440';

DROP TABLE IF EXISTS t2;
CREATE TABLE t2(c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t2 VALUES (928386547), (1541944097), (2086579505), (1990427322), (-542998757), (390253678), (554855248), (203290629), (1504693323);

SELECT
    MAX(left.c0),
    min2(left.c0, -(-left.c0) * (radians(left.c0) - radians(left.c0))) AS g,
    (((-1925024212 IS NOT NULL) IS NOT NULL) != radians(tan(1216286224))) AND cos(lcm(MAX(left.c0), -1966575216) OR (MAX(left.c0) * 1180517420)) AS h,
    NOT h,
    h IS NULL
FROM t2 AS left
GROUP BY g
ORDER BY g DESC;

SELECT '=';

SELECT MAX(left.c0), min2(left.c0, -(-left.c0) * (radians(left.c0) - radians(left.c0))) as g, (((-1925024212 IS NOT NULL) IS NOT NULL) != radians(tan(1216286224))) AND cos(lcm(MAX(left.c0), -1966575216) OR (MAX(left.c0) * 1180517420)) as h, not h, h is null
                  FROM t2 AS left
                  GROUP BY g HAVING h ORDER BY g DESC SETTINGS enable_optimize_predicate_expression = 0;
SELECT  '=';

SELECT MAX(left.c0), min2(left.c0, -(-left.c0) * (radians(left.c0) - radians(left.c0))) as g, (((-1925024212 IS NOT NULL) IS NOT NULL) != radians(tan(1216286224))) AND cos(lcm(MAX(left.c0), -1966575216) OR (MAX(left.c0) * 1180517420)) as h, not h, h is null
                  FROM t2 AS left
                  GROUP BY g HAVING h ORDER BY g DESC SETTINGS enable_optimize_predicate_expression = 1;

DROP TABLE IF EXISTS t2;
