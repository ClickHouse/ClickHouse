SET optimize_using_constraints = 1, convert_query_to_cnf = 1, enable_analyzer=1;
CREATE TABLE t0 (c0 String, c1 String, CONSTRAINT c ASSUME (c0 = '2000-01-01 00:00:00'::DateTime64 AND c1 = '')) ENGINE = Memory;
SELECT 1 FROM t0 WHERE t0.c0 = t0.c0; -- { serverError TYPE_MISMATCH }
