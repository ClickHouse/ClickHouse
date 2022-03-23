DROP TABLE IF EXISTS 02244_rptable;
CREATE TABLE 02244_rptable (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO 02244_rptable SELECT number FROM numbers(0, 16);

DROP ROW POLICY IF EXISTS 02244_filter_1 ON 02244_rptable;
DROP ROW POLICY IF EXISTS 02244_filter_2 ON 02244_rptable;
DROP ROW POLICY IF EXISTS 02244_filter_3 ON 02244_rptable;
DROP ROW POLICY IF EXISTS 02244_filter_4 ON 02244_rptable;

SELECT 'None';
SELECT * FROM 02244_rptable;

CREATE ROW POLICY 02244_filter_1 ON 02244_rptable USING x > 2 AS permissive TO ALL;
SELECT 'R1: x > 2';
SELECT * FROM 02244_rptable;

CREATE ROW POLICY 02244_filter_2 ON 02244_rptable USING x < 13 AS restrictive TO ALL;
SELECT 'R1, R2: (x > 2) AND (x < 13)';
SELECT * FROM 02244_rptable;

CREATE ROW POLICY 02244_filter_3 ON 02244_rptable USING x%2 = 0 AS simple TO ALL;
SELECT 'R1, R2, R3: (x > 2) AND (x < 13) AND (x%2 = 0)';
SELECT * FROM 02244_rptable;

CREATE ROW POLICY 02244_filter_4 ON 02244_rptable USING x%3 = 0 AS simple TO ALL;
SELECT 'R1, R2, R3, R4: (x > 2) AND (x < 13) AND (x%2 = 0) AND (x%3 = 0)';
SELECT * FROM 02244_rptable;

DROP ROW POLICY 02244_filter_1 ON 02244_rptable;
SELECT 'R2, R3, R4: FALSE(no permissive filters) AND (x < 13) AND (x%2 = 0) AND (x%3 = 0)';
SELECT * FROM 02244_rptable;

DROP ROW POLICY 02244_filter_2 ON 02244_rptable;
SELECT 'R3, R4: (x%2 = 0) AND (x%3 = 0)';
SELECT * FROM 02244_rptable;

DROP ROW POLICY 02244_filter_3 ON 02244_rptable;
SELECT 'R4: (x%3 = 0)';
SELECT * FROM 02244_rptable;

DROP ROW POLICY 02244_filter_4 ON 02244_rptable;
SELECT 'None';
SELECT * FROM 02244_rptable;

DROP TABLE 02244_rptable;
