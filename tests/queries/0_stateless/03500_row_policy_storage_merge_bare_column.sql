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
