-- https://s3.amazonaws.com/clickhouse-test-reports/0/a02b20a9813c6ba0880c67f079363ef1c5440109/sqlancer__debug_.html
-- Caused by enablement of query_plan_merge_filters. Will fail if the next line is uncommented
-- set query_plan_merge_filters=1;

CREATE TABLE IF NOT EXISTS t3 (c0 Int32) ENGINE = Memory() ;
INSERT INTO t3(c0) VALUES (1110866669);

-- These 2 queries are expected to return the same
SELECT (tan (t3.c0)), SUM(-1017248723), ((t3.c0)%(t3.c0)) FROM t3 GROUP BY t3.c0 SETTINGS aggregate_functions_null_for_empty=1, enable_optimize_predicate_expression=0;
SELECT (tan (t3.c0)), SUM(-1017248723), ((t3.c0)%(t3.c0)) FROM t3 GROUP BY t3.c0 HAVING ((tan ((- (SUM(-1017248723)))))) and ((sqrt (SUM(-1017248723)))) UNION ALL SELECT (tan (t3.c0)), SUM(-1017248723), ((t3.c0)%(t3.c0)) FROM t3 GROUP BY t3.c0 HAVING (NOT (((tan ((- (SUM(-1017248723)))))) and ((sqrt (SUM(-1017248723)))))) UNION ALL SELECT (tan (t3.c0)), SUM(-1017248723), ((t3.c0)%(t3.c0)) FROM t3 GROUP BY t3.c0 HAVING ((((tan ((- (SUM(-1017248723)))))) and ((sqrt (SUM(-1017248723))))) IS NULL) SETTINGS aggregate_functions_null_for_empty=1, enable_optimize_predicate_expression=0;
