SET optimize_substitute_columns = 1, convert_query_to_cnf = 1, enable_analyzer = 1;

CREATE TABLE t_cnf_subquery_subst (a Int64, d Int32, CONSTRAINT c1 ASSUME a = d) ENGINE = TinyLog;

SELECT anyLastDistinct((SELECT 1 WHERE a = 1)) FROM t_cnf_subquery_subst WHERE isNotNull(a); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_cnf_subquery_subst;
