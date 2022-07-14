DROP TABLE IF EXISTS _02131_rptable;
CREATE TABLE _02131_rptable (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO _02131_rptable VALUES (1), (2), (3), (4);

DROP ROW POLICY IF EXISTS _02131_filter_1 ON _02131_rptable;
DROP ROW POLICY IF EXISTS _02131_filter_2 ON _02131_rptable;
DROP ROW POLICY IF EXISTS _02131_filter_3 ON _02131_rptable;
DROP ROW POLICY IF EXISTS _02131_filter_4 ON _02131_rptable;
DROP ROW POLICY IF EXISTS _02131_filter_5 ON _02131_rptable;

SELECT 'None';
SELECT * FROM _02131_rptable;

CREATE ROW POLICY _02131_filter_1 ON _02131_rptable USING x=1 AS permissive TO ALL;
SELECT 'R1: x == 1';
SELECT * FROM _02131_rptable;

CREATE ROW POLICY _02131_filter_2 ON _02131_rptable USING x=2 AS permissive TO ALL;
SELECT 'R1, R2: (x == 1) OR (x == 2)';
SELECT * FROM _02131_rptable;

CREATE ROW POLICY _02131_filter_3 ON _02131_rptable USING x=3 AS permissive TO ALL;
SELECT 'R1, R2, R3: (x == 1) OR (x == 2) OR (x == 3)';
SELECT * FROM _02131_rptable;

CREATE ROW POLICY _02131_filter_4 ON _02131_rptable USING x<=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2)';
SELECT * FROM _02131_rptable;

CREATE ROW POLICY _02131_filter_5 ON _02131_rptable USING x>=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4, R5: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rptable;

DROP ROW POLICY _02131_filter_1 ON _02131_rptable;
SELECT 'R2, R3, R4, R5: ((x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rptable;

DROP ROW POLICY _02131_filter_2 ON _02131_rptable;
SELECT 'R3, R4, R5: (x == 3) AND (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rptable;

DROP ROW POLICY _02131_filter_3 ON _02131_rptable;
SELECT 'R4, R5: (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rptable;

DROP ROW POLICY _02131_filter_4 ON _02131_rptable;
SELECT 'R5: (x >= 2)';
SELECT * FROM _02131_rptable;

DROP ROW POLICY _02131_filter_5 ON _02131_rptable;
SELECT 'None';
SELECT * FROM _02131_rptable;

DROP TABLE _02131_rptable;
