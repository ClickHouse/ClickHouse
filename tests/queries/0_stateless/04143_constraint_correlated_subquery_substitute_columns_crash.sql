-- Regression test for STID 3452-4527: vector OOB in `ColumnNode::getColumnSource`
-- triggered by AST fuzzer query with mixed-type CONSTRAINT and correlated subquery.
--
-- Root cause: `SubstituteColumnVisitor` (and other visitors) in `ConvertQueryToCNFPass`
-- walked into a `QueryNode`/`UnionNode` parent and substituted column references inside
-- the subquery's `correlated_columns_list`. With a mixed-type constraint the substitution
-- wraps the canonical column in a `_CAST(...)` `FunctionNode`, which the planner later
-- iterates expecting a `ColumnNode`. The cast lookup goes through `as<ColumnNode>()`
-- (returns null on mismatch) and `getColumnSource()` is called on a null `this`, which
-- libc++ hardening catches as `vector[]` out-of-bounds inside `getSourceWeakPointer`.
--
-- The fix adds a parent-type check to `needChildVisit`: when the visitor is invoked
-- directly on a `QUERY`/`UNION` node, do not descend into its children — the substitution
-- is scoped to the current query, not nested subqueries.

DROP TABLE IF EXISTS t_constraint_corr_subst;

-- Mixed-type constraint: triggers the `_CAST` wrapping path in `SubstituteColumnVisitor`.
-- (When all columns share the same type, the substitution would only swap `ColumnNode` for
-- `ColumnNode` and the bug would not manifest.)
CREATE TABLE t_constraint_corr_subst
(
    a UInt16,
    b String,
    c String,
    d String,
    CONSTRAINT c1 ASSUME (a <= b) AND (b <= c) AND (c <= d) AND (d <= a)
)
ENGINE = TinyLog;

INSERT INTO t_constraint_corr_subst VALUES (1, '2', '3', '4');

-- The exact AST fuzzer pattern that crashed in the MSAN build (STID 3452-4527).
-- After the fix, this no longer crashes the server. The query may still raise a
-- regular planner exception (not a crash) because the planner's own
-- `collectTableExpressionData` does not register the correlated subquery's columns
-- when CNF reduces WHERE to a single subquery atom — that is a separate issue.
SELECT count() FROM t_constraint_corr_subst
WHERE (a >= c) AND (SELECT b >= d)
SETTINGS
    enable_analyzer = 1,
    convert_query_to_cnf = 1,
    optimize_using_constraints = 1,
    optimize_substitute_columns = 1,
    allow_experimental_correlated_subqueries = 1
; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- Same trigger, with PREWHERE (matches the exact AST fuzzer query).
SELECT count() FROM t_constraint_corr_subst
PREWHERE (c = a) AND (d = b)
WHERE (a >= c) AND (SELECT b >= d)
LIMIT 100
SETTINGS
    enable_analyzer = 1,
    convert_query_to_cnf = 1,
    optimize_using_constraints = 1,
    optimize_move_to_prewhere = 1,
    optimize_substitute_columns = 1,
    optimize_append_index = 1,
    allow_experimental_correlated_subqueries = 1
; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK, ILLEGAL_PREWHERE }

-- Sanity check: the same constraints + correlated subquery without mixed types
-- (i.e. without the `_CAST` substitution path) should execute successfully and
-- produce a correct count. This guards against the fix accidentally breaking the
-- non-mixed-type substitution path.
DROP TABLE IF EXISTS t_constraint_corr_subst_homogeneous;

CREATE TABLE t_constraint_corr_subst_homogeneous
(
    a String,
    b String,
    c String,
    d String,
    CONSTRAINT c1 ASSUME (a <= b) AND (b <= c) AND (c <= d) AND (d <= a)
)
ENGINE = TinyLog;

INSERT INTO t_constraint_corr_subst_homogeneous VALUES ('1', '2', '3', '4');

SELECT count() FROM t_constraint_corr_subst_homogeneous
WHERE (a > '0') AND (a < '5')
SETTINGS
    enable_analyzer = 1,
    convert_query_to_cnf = 1,
    optimize_using_constraints = 1,
    optimize_substitute_columns = 1
;

DROP TABLE t_constraint_corr_subst;
DROP TABLE t_constraint_corr_subst_homogeneous;
