SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x Nullable(Int32), y Nullable(Int32)) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 1), (2, 2), (NULL, NULL);

CREATE TABLE t2 (x Nullable(Int32), y Nullable(Int32)) ENGINE = Memory;
INSERT INTO t2 VALUES (2, 2), (3, 3), (NULL, NULL);

SELECT e2 FROM t1 FULL OUTER JOIN t2
ON (
    (((t1.y = t2.y) OR ((t1.y IS NULL) AND (t2.y IS NULL))) AND (COALESCE(t1.x, 0) != 2))
    OR (((t1.x = t2.x)) AND ((t2.x IS NOT NULL) AND (t1.x IS NOT NULL)))
    AS e2
)
ORDER BY ALL;

SELECT *, e2 FROM t1 FULL OUTER JOIN t2
ON (
    (((t1.y = t2.y) OR ((t1.y IS NULL) AND (t2.y IS NULL))) AND (COALESCE(t1.x, 0) != 2))
    OR (((t1.x = t2.x)) AND ((t2.x IS NOT NULL) AND (t1.x IS NOT NULL)))
    AS e2
) AND (t1.x = 1) AND (t2.x = 1)
ORDER BY ALL;
