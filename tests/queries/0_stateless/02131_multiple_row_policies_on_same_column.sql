DROP TABLE IF EXISTS 02131_multiple_row_policies_on_same_column;
CREATE TABLE 02131_multiple_row_policies_on_same_column (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO 02131_multiple_row_policies_on_same_column VALUES (1), (2), (3), (4);

DROP ROW POLICY IF EXISTS 02131_filter_1 ON 02131_multiple_row_policies_on_same_column;
DROP ROW POLICY IF EXISTS 02131_filter_2 ON 02131_multiple_row_policies_on_same_column;
DROP ROW POLICY IF EXISTS 02131_filter_3 ON 02131_multiple_row_policies_on_same_column;
DROP ROW POLICY IF EXISTS 02131_filter_4 ON 02131_multiple_row_policies_on_same_column;
DROP ROW POLICY IF EXISTS 02131_filter_5 ON 02131_multiple_row_policies_on_same_column;

SELECT 'None';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

CREATE ROW POLICY 02131_filter_1 ON 02131_multiple_row_policies_on_same_column USING x=1 AS permissive TO ALL;
SELECT 'R1: x == 1';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

CREATE ROW POLICY 02131_filter_2 ON 02131_multiple_row_policies_on_same_column USING x=2 AS permissive TO ALL;
SELECT 'R1, R2: (x == 1) OR (x == 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

CREATE ROW POLICY 02131_filter_3 ON 02131_multiple_row_policies_on_same_column USING x=3 AS permissive TO ALL;
SELECT 'R1, R2, R3: (x == 1) OR (x == 2) OR (x == 3)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

CREATE ROW POLICY 02131_filter_4 ON 02131_multiple_row_policies_on_same_column USING x<=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

CREATE ROW POLICY 02131_filter_5 ON 02131_multiple_row_policies_on_same_column USING x>=2 AS simple TO ALL;
SELECT 'R1, R2, R3, R4, R5: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

DROP ROW POLICY 02131_filter_1 ON 02131_multiple_row_policies_on_same_column;
SELECT 'R2, R3, R4, R5: ((x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

DROP ROW POLICY 02131_filter_2 ON 02131_multiple_row_policies_on_same_column;
SELECT 'R3, R4, R5: (x == 3) AND (x <= 2) AND (x >= 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

DROP ROW POLICY 02131_filter_3 ON 02131_multiple_row_policies_on_same_column;
SELECT 'R4, R5: FALSE(no permissive filters) AND (x <= 2) AND (x >= 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

DROP ROW POLICY 02131_filter_4 ON 02131_multiple_row_policies_on_same_column;
SELECT 'R5: (x >= 2)';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

DROP ROW POLICY 02131_filter_5 ON 02131_multiple_row_policies_on_same_column;
SELECT 'None';
SELECT * FROM 02131_multiple_row_policies_on_same_column;

DROP TABLE 02131_multiple_row_policies_on_same_column;
