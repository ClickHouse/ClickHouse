SET optimize_substitute_columns = 1, convert_query_to_cnf = 1, enable_analyzer = 1;

CREATE TABLE t_cnf_subquery_subst (a Int64, d Int32, CONSTRAINT c1 ASSUME a = d) ENGINE = TinyLog;

-- Used to produce: Logical error: 'Bad cast from type DB::FunctionNode to DB::ColumnNode'
-- Now consistently raises NOT_IMPLEMENTED (same as Memory/MergeTree without the bug).
SELECT anyLastDistinct((SELECT 1 WHERE a = 1)) FROM t_cnf_subquery_subst WHERE isNotNull(a); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_cnf_subquery_subst;
