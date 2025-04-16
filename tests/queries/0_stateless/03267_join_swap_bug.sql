DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);

SELECT 1 FROM t0 PASTE JOIN (SELECT 1 c0) tx PASTE JOIN t0 t1 GROUP BY tx.c0;
SELECT count() FROM t0 PASTE JOIN (SELECT 1 c0) tx PASTE JOIN t0 t1 GROUP BY tx.c0;

SET allow_experimental_analyzer = 1;

SELECT *
FROM
(
    SELECT *
    FROM system.one
) AS a
INNER JOIN
(
    SELECT *
    FROM system.one
) AS b USING (dummy)
INNER JOIN
(
    SELECT *
    FROM system.one
) AS c USING (dummy)
SETTINGS join_algorithm = 'full_sorting_merge';


SELECT count(1)
FROM ( SELECT 1 AS x, x ) AS t1
RIGHT JOIN (SELECT materialize(2) AS x) AS t2
ON t1.x = t2.x
;
