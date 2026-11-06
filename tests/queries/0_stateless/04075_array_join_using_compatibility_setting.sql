DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (b Int, c0 Map(Array(Int),Int)) ENGINE = Memory;
INSERT INTO t1 VALUES (10, {[1]:2, [3]:4});

SET enable_analyzer = 1;

SELECT b + 100 AS b FROM t1 ARRAY JOIN t1.c0.keys AS x JOIN (SELECT 10 AS b) y USING (b) ORDER BY b
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

DROP TABLE t1;
