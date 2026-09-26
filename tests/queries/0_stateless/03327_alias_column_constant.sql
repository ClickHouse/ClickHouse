
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a Int32, b Int32 ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (1), (2), (3);

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a Int32, b Int32 ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2 VALUES (2), (3), (4);

-- { echoOn }

SELECT b FROM t1;
SELECT b FROM t1 JOIN t2 USING b;
SELECT 1 AS b FROM t1 JOIN t2 USING b;
SELECT 1 AS b FROM t1 JOIN t2 USING b SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT 2 AS a FROM t1 JOIN t2 USING a SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;


