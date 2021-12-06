DROP TABLE IF EXISTS 02131_multiply_row_policies_on_same_column;
CREATE TABLE 02131_multiply_row_policies_on_same_column (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO 02131_multiply_row_policies_on_same_column VALUES (1), (2), (3), (4);


DROP ROW POLICY IF EXISTS 02131_filter_1 ON 02131_multiply_row_policies_on_same_column;
DROP ROW POLICY IF EXISTS 02131_filter_2 ON 02131_multiply_row_policies_on_same_column;
DROP ROW POLICY IF EXISTS 02131_filter_3 ON 02131_multiply_row_policies_on_same_column;
SELECT count() FROM 02131_multiply_row_policies_on_same_column;


CREATE ROW POLICY 02131_filter_1 ON 02131_multiply_row_policies_on_same_column USING x=1 TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column;
CREATE ROW POLICY 02131_filter_2 ON 02131_multiply_row_policies_on_same_column USING x=2 TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column;
CREATE ROW POLICY 02131_filter_3 ON 02131_multiply_row_policies_on_same_column USING x=3 TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column;
SELECT count() FROM 02131_multiply_row_policies_on_same_column;


DROP ROW POLICY 02131_filter_1 ON 02131_multiply_row_policies_on_same_column;
DROP ROW POLICY 02131_filter_2 ON 02131_multiply_row_policies_on_same_column;
DROP ROW POLICY 02131_filter_3 ON 02131_multiply_row_policies_on_same_column;
SELECT count() FROM 02131_multiply_row_policies_on_same_column;


DROP TABLE IF EXISTS 02131_multiply_row_policies_on_same_column_1;
CREATE TABLE 02131_multiply_row_policies_on_same_column_1 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO 02131_multiply_row_policies_on_same_column_1 VALUES (1), (2), (3), (4);

DROP ROW POLICY IF EXISTS 02131_filter_0_1 ON 02131_multiply_row_policies_on_same_column_1;
DROP ROW POLICY IF EXISTS 02131_filter_1_1 ON 02131_multiply_row_policies_on_same_column_1;
DROP ROW POLICY IF EXISTS 02131_filter_2_1 ON 02131_multiply_row_policies_on_same_column_1;
DROP ROW POLICY IF EXISTS 02131_filter_3_1 ON 02131_multiply_row_policies_on_same_column_1;
SELECT count() FROM 02131_multiply_row_policies_on_same_column_1;

CREATE ROW POLICY 02131_filter_0_1 ON 02131_multiply_row_policies_on_same_column_1 USING 1 TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column_1;
CREATE ROW POLICY 02131_filter_1_1 ON 02131_multiply_row_policies_on_same_column_1 USING x>1 AS RESTRICTIVE TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column_1;
CREATE ROW POLICY 02131_filter_2_1 ON 02131_multiply_row_policies_on_same_column_1 USING x>2 AS RESTRICTIVE TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column_1;
CREATE ROW POLICY 02131_filter_3_1 ON 02131_multiply_row_policies_on_same_column_1 USING x>3 AS RESTRICTIVE TO ALL;
SELECT count() FROM 02131_multiply_row_policies_on_same_column_1;
SELECT count() FROM 02131_multiply_row_policies_on_same_column_1;

DROP TABLE 02131_multiply_row_policies_on_same_column_1;
DROP ROW POLICY 02131_filter_1_1 ON 02131_multiply_row_policies_on_same_column_1;
DROP ROW POLICY 02131_filter_2_1 ON 02131_multiply_row_policies_on_same_column_1;
DROP ROW POLICY 02131_filter_3_1 ON 02131_multiply_row_policies_on_same_column_1;
