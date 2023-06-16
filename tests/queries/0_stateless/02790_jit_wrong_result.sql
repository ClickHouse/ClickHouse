DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (c0 Int32, c1 Int32, c2 String) ENGINE = Log() ;
INSERT INTO t2(c1, c0) VALUES (1697596429, 1259570390);
INSERT INTO t2(c1, c2) VALUES (-871444251, 's,');
INSERT INTO t2(c0, c2, c1) VALUES (-943999939, '', 1756486294);

SELECT MIN(t2.c0)
FROM t2
GROUP BY log(-(t2.c0 / (t2.c0 - t2.c0)))
HAVING NOT (NOT (-(NOT MIN(t2.c0))))
UNION ALL
SELECT MIN(t2.c0)
FROM t2
GROUP BY log(-(t2.c0 / (t2.c0 - t2.c0)))
HAVING NOT (NOT (NOT (-(NOT MIN(t2.c0)))))
UNION ALL
SELECT MIN(t2.c0)
FROM t2
GROUP BY log(-(t2.c0 / (t2.c0 - t2.c0)))
HAVING (NOT (NOT (-(NOT MIN(t2.c0))))) IS NULL
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0, min_count_to_compile_expression = 0;

DROP TABLE t2;
