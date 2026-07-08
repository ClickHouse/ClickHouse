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
