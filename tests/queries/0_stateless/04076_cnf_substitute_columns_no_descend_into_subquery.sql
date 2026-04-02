-- Regression test: substituteColumns in ConvertQueryToCNFPass must not descend into
-- subqueries when collecting constraint components or performing substitutions.
--
-- Previously, ComponentCollectorVisitor and SubstituteColumnVisitor would recursively visit
-- all children of inner QueryNode nodes, including the correlated_columns list (child index 16).
-- When a type-mismatch constraint (e.g. ASSUME a = d with a Int64 and d Int32) caused the
-- optimizer to select a CAST expression as the representative for a constraint component,
-- SubstituteColumnVisitor replaced ColumnNode(a) in correlated_columns with
-- FunctionNode(CAST(d, Int64)). This caused a logical exception in the Planner because
-- collectTopLevelColumnIdentifiers expects correlated columns to always be ColumnNode objects.
-- After the fix, the query correctly produces NOT_IMPLEMENTED instead of a logical exception.

SET optimize_substitute_columns = 1, convert_query_to_cnf = 1;

CREATE TABLE t_cnf_subquery_subst (a Int64, d Int32, CONSTRAINT c1 ASSUME a = d) ENGINE = TinyLog;

-- Used to produce: Logical error: 'Bad cast from type DB::FunctionNode to DB::ColumnNode'
-- Now consistently raises NOT_IMPLEMENTED (same as Memory/MergeTree without the bug).
SELECT anyLastDistinct((SELECT 1 WHERE a = 1)) FROM t_cnf_subquery_subst WHERE isNotNull(a); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_cnf_subquery_subst;
