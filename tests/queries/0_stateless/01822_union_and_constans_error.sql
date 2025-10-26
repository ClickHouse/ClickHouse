drop table if exists t0;
CREATE TABLE t0 (c0 String) ENGINE = Log();

SELECT isNull(t0.c0) OR COUNT('\n?pVa')
FROM t0
GROUP BY t0.c0
HAVING isNull(t0.c0)
UNION ALL
SELECT isNull(t0.c0) OR COUNT('\n?pVa')
FROM t0
GROUP BY t0.c0
HAVING NOT isNull(t0.c0)
UNION ALL
SELECT isNull(t0.c0) OR COUNT('\n?pVa')
FROM t0
GROUP BY t0.c0
HAVING isNull(isNull(t0.c0))
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0 format Null;

drop table if exists t0;
