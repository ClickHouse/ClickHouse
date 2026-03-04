DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Map(Array(Int),Int)) ENGINE = Memory;

SET enable_analyzer = 1;

SELECT 1 c1 FROM t0 ARRAY JOIN t0.c0.keys AS a0 JOIN (SELECT 1 c1) y USING (c1)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError UNKNOWN_IDENTIFIER }
