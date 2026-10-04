-- A row policy whose filter is a bare existing column (e.g. USING flag) used to abort a
-- SELECT over a Merge table with "Cannot determine row level filter; 0 columns deleted,
-- 0 columns added": the filter actions output coincided with an input column, so the
-- output-minus-inputs diff was empty. The filter column is now taken from the expression
-- itself, matching the non-Merge row-policy path.

DROP TABLE IF EXISTS 03500_t;
DROP TABLE IF EXISTS 03500_m;

CREATE TABLE 03500_t (a Int32, flag UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t VALUES (1, 1), (2, 0), (3, 1), (4, 0);
CREATE TABLE 03500_m AS 03500_t ENGINE = Merge(currentDatabase(), '03500_t');

CREATE ROW POLICY 03500_p_bare ON 03500_t USING flag AS PERMISSIVE TO ALL;
SELECT 'bare column policy (was crash)';
SELECT * FROM 03500_m ORDER BY a;
SELECT 'filter column not in select list';
SELECT a FROM 03500_m ORDER BY a;
DROP ROW POLICY 03500_p_bare ON 03500_t;

CREATE ROW POLICY 03500_p_expr ON 03500_t USING a > 1 AS PERMISSIVE TO ALL;
SELECT 'expression policy';
SELECT * FROM 03500_m ORDER BY a;
DROP ROW POLICY 03500_p_expr ON 03500_t;

CREATE ROW POLICY 03500_p_and ON 03500_t USING flag AND a < 3 AS PERMISSIVE TO ALL;
SELECT 'compound policy';
SELECT * FROM 03500_m ORDER BY a;
DROP ROW POLICY 03500_p_and ON 03500_t;

DROP TABLE 03500_m;
DROP TABLE 03500_t;

-- The helper alias generated for the filter column must not clash with a real column of the
-- child table. Here the child legitimately has a column named __row_policy_filter of a different
-- type than the filter, and SELECT * reads it; the alias must be renamed to avoid a duplicate
-- column name when building the filtered block.
DROP TABLE IF EXISTS 03500_t2;
DROP TABLE IF EXISTS 03500_m2;

CREATE TABLE 03500_t2 (a Int32, flag UInt8, `__row_policy_filter` String) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t2 VALUES (1, 1, 'x'), (2, 0, 'y'), (3, 1, 'z');
CREATE TABLE 03500_m2 AS 03500_t2 ENGINE = Merge(currentDatabase(), '03500_t2');

CREATE ROW POLICY 03500_p_clash ON 03500_t2 USING flag AS PERMISSIVE TO ALL;
SELECT 'name clash with real __row_policy_filter column';
SELECT * FROM 03500_m2 ORDER BY a;
DROP ROW POLICY 03500_p_clash ON 03500_t2;

DROP TABLE 03500_m2;
DROP TABLE 03500_t2;

-- The filter actions must not leak the raw policy predicate (e.g. greater(a, 1)) into the stream:
-- the post-filter outputs are the source columns plus the helper alias only. Without that, with
-- query_plan_enable_optimizations = 0 (which skips the unused-column pruning that would otherwise
-- drop it) a single table leaks the synthetic column, and a Merge over children with different
-- policies fails in Pipe::unitePipes on mismatched headers.
DROP TABLE IF EXISTS 03500_t3;
DROP TABLE IF EXISTS 03500_t4;
DROP TABLE IF EXISTS 03500_m3;

CREATE TABLE 03500_t3 (a Int32, flag UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t3 VALUES (1, 1), (2, 0), (3, 1), (4, 0);
CREATE TABLE 03500_t4 (a Int32, flag UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t4 VALUES (1, 1), (2, 0), (3, 1), (4, 0);
CREATE TABLE 03500_m3 AS 03500_t3 ENGINE = Merge(currentDatabase(), '03500_t3|03500_t4');

CREATE ROW POLICY 03500_p_expr3 ON 03500_t3 USING a > 1 AS PERMISSIVE TO ALL;
CREATE ROW POLICY 03500_p_flag4 ON 03500_t4 USING flag AS PERMISSIVE TO ALL;
SELECT 'no optimizations, different policies per child';
SELECT * FROM 03500_m3 ORDER BY a, flag SETTINGS query_plan_enable_optimizations = 0;
SELECT 'no optimizations, single column';
SELECT a FROM 03500_m3 ORDER BY a, flag SETTINGS query_plan_enable_optimizations = 0;
-- The raw predicate (greater(a, 1)) must not survive past the row-policy filter into the child
-- stream, even with plan optimizations disabled; only the source columns pass through.
SELECT 'no leaked predicate column in the plan';
SELECT count() FROM (EXPLAIN header = 1 SELECT * FROM 03500_m3 SETTINGS query_plan_enable_optimizations = 0) WHERE explain ILIKE '%greater(a, 1)%';
DROP ROW POLICY 03500_p_expr3 ON 03500_t3;
DROP ROW POLICY 03500_p_flag4 ON 03500_t4;

DROP TABLE 03500_m3;
DROP TABLE 03500_t4;
DROP TABLE 03500_t3;

-- A query-level column alias equal to the helper name (e.g. SELECT flag AS __row_policy_filter)
-- is applied in the outer plan above the Merge child read, so it never enters the child stream
-- the row-policy filter operates on. The query returns correctly filtered rows and does not throw
-- AMBIGUOUS_COLUMN_NAME, both with and without plan optimizations, single and multiple children.
DROP TABLE IF EXISTS 03500_t5;
DROP TABLE IF EXISTS 03500_t6;
DROP TABLE IF EXISTS 03500_m5;

CREATE TABLE 03500_t5 (a Int32, flag UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t5 VALUES (1, 1), (2, 0), (3, 1), (4, 0);
CREATE TABLE 03500_t6 (a Int32, flag UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t6 VALUES (1, 1), (2, 0), (3, 1), (4, 0);
CREATE TABLE 03500_m5 AS 03500_t5 ENGINE = Merge(currentDatabase(), '03500_t5|03500_t6');

CREATE ROW POLICY 03500_p_alias5 ON 03500_t5 USING a > 1 AS PERMISSIVE TO ALL;
CREATE ROW POLICY 03500_p_alias6 ON 03500_t6 USING flag AS PERMISSIVE TO ALL;
SELECT 'query alias equal to helper name';
SELECT flag AS __row_policy_filter FROM 03500_m5 ORDER BY a, flag;
SELECT 'query alias equal to helper name, no optimizations';
SELECT flag AS __row_policy_filter FROM 03500_m5 ORDER BY a, flag SETTINGS query_plan_enable_optimizations = 0;
DROP ROW POLICY 03500_p_alias5 ON 03500_t5;
DROP ROW POLICY 03500_p_alias6 ON 03500_t6;

DROP TABLE 03500_m5;
DROP TABLE 03500_t6;
DROP TABLE 03500_t5;

-- A child table may have a real quoted column whose name equals the policy predicate's column name
-- (e.g. `greater(a, 1)` for USING a > 1). The helper alias binds to the computed predicate node, not
-- to that user column: the row-policy actions DAG's inputs are only the columns the predicate
-- references (a), so the user column is not a node there and cannot shadow the predicate in
-- findInOutputs. The Merge filters on a > 1, not on the user column, so no silent wrong result and no
-- AMBIGUOUS_COLUMN_NAME. Values are chosen so a mis-bind would filter on the user column and return a
-- different set (a = 0, 1) than the correct a > 1 (a = 2, 3).
DROP TABLE IF EXISTS 03500_t7;
DROP TABLE IF EXISTS 03500_m7;

CREATE TABLE 03500_t7 (a Int32, `greater(a, 1)` UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t7 VALUES (0, 1), (1, 1), (2, 0), (3, 0);
CREATE TABLE 03500_m7 AS 03500_t7 ENGINE = Merge(currentDatabase(), '03500_t7');

CREATE ROW POLICY 03500_p_quoted7 ON 03500_t7 USING a > 1 AS PERMISSIVE TO ALL;
SELECT 'quoted column equal to predicate name, filters on a>1 not the column';
SELECT * FROM 03500_m7 ORDER BY a;
SELECT 'quoted column, single column';
SELECT a FROM 03500_m7 ORDER BY a;
SELECT 'quoted column, no optimizations';
SELECT a FROM 03500_m7 ORDER BY a SETTINGS query_plan_enable_optimizations = 0;
DROP ROW POLICY 03500_p_quoted7 ON 03500_t7;

DROP TABLE 03500_m7;
DROP TABLE 03500_t7;

-- Same as above with a String-typed quoted column: a mis-bind would put a String where a UInt8 filter
-- is expected and throw. It must instead filter on a > 1 and pass.
DROP TABLE IF EXISTS 03500_t8;
DROP TABLE IF EXISTS 03500_m8;

CREATE TABLE 03500_t8 (a Int32, `greater(a, 1)` String) ENGINE = MergeTree ORDER BY a;
INSERT INTO 03500_t8 VALUES (0, 'p'), (1, 'q'), (2, 'r'), (3, 's');
CREATE TABLE 03500_m8 AS 03500_t8 ENGINE = Merge(currentDatabase(), '03500_t8');

CREATE ROW POLICY 03500_p_quoted8 ON 03500_t8 USING a > 1 AS PERMISSIVE TO ALL;
SELECT 'quoted string column equal to predicate name';
SELECT * FROM 03500_m8 ORDER BY a;
DROP ROW POLICY 03500_p_quoted8 ON 03500_t8;

DROP TABLE 03500_m8;
DROP TABLE 03500_t8;
