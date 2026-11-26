DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c0 Int32, PRIMARY KEY (c0)) ENGINE=MergeTree;
INSERT INTO t1 VALUES (1554690688);

select '=';

SELECT MIN(t1.c0)
FROM t1
GROUP BY
    (-sign(cos(t1.c0))) * (-max2(t1.c0, t1.c0 / t1.c0)),
    t1.c0 * t1.c0,
    sign(-exp(-t1.c0))
HAVING -(-(MIN(t1.c0) + MIN(t1.c0))) AND (pow('{b' > '-657301241', log(-1004522121)) IS NOT NULL)
UNION ALL
SELECT MIN(t1.c0)
FROM t1
GROUP BY
    (-sign(cos(t1.c0))) * (-max2(t1.c0, t1.c0 / t1.c0)),
    t1.c0 * t1.c0,
    sign(-exp(-t1.c0))
HAVING NOT (-(-(MIN(t1.c0) + MIN(t1.c0))) AND (pow('{b' > '-657301241', log(-1004522121)) IS NOT NULL))
UNION ALL
SELECT MIN(t1.c0)
FROM t1
GROUP BY
    (-sign(cos(t1.c0))) * (-max2(t1.c0, t1.c0 / t1.c0)),
    t1.c0 * t1.c0,
    sign(-exp(-t1.c0))
HAVING (-(-(MIN(t1.c0) + MIN(t1.c0))) AND (pow('{b' > '-657301241', log(-1004522121)) IS NOT NULL)) IS NULL
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

select '=';

SELECT MIN(t1.c0)
FROM t1
GROUP BY t1.c0
HAVING and(MIN(t1.c0) + MIN(t1.c0), 1)
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

select '=';

DROP TABLE IF EXISTS t1;
