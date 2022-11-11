DROP TABLE IF EXISTS rqtable_02131;
CREATE TABLE rqtable_02131 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO rqtable_02131 VALUES (1), (2), (3), (4);

DROP ROW POLICY IF EXISTS filter_02131_1 ON rqtable_02131;
DROP ROW POLICY IF EXISTS filter_02131_2 ON rqtable_02131;
DROP ROW POLICY IF EXISTS filter_02131_3 ON rqtable_02131;
DROP ROW POLICY IF EXISTS filter_02131_4 ON rqtable_02131;
DROP ROW POLICY IF EXISTS filter_02131_5 ON rqtable_02131;

SELECT 'None';
SELECT * FROM rqtable_02131;

CREATE ROW POLICY filter_02131_1 ON rqtable_02131 USING x=1 AS permissive TO ALL;
SELECT 'R1: x == 1';
SELECT * FROM rqtable_02131;

CREATE ROW POLICY filter_02131_2 ON rqtable_02131 USING x=2 AS permissive TO ALL;
SELECT 'R1, R2: (x == 1) OR (x == 2)';
SELECT * FROM rqtable_02131;

CREATE ROW POLICY filter_02131_3 ON rqtable_02131 USING x=3 AS permissive TO ALL;
SELECT 'R1, R2, R3: (x == 1) OR (x == 2) OR (x == 3)';
SELECT * FROM rqtable_02131;

CREATE ROW POLICY filter_02131_4 ON rqtable_02131 USING x<=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2)';
SELECT * FROM rqtable_02131;

CREATE ROW POLICY filter_02131_5 ON rqtable_02131 USING x>=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4, R5: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM rqtable_02131;

DROP ROW POLICY filter_02131_1 ON rqtable_02131;
SELECT 'R2, R3, R4, R5: ((x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM rqtable_02131;

DROP ROW POLICY filter_02131_2 ON rqtable_02131;
SELECT 'R3, R4, R5: (x == 3) AND (x <= 2) AND (x >= 2)';
SELECT * FROM rqtable_02131;

DROP ROW POLICY filter_02131_3 ON rqtable_02131;
SELECT 'R4, R5: (x <= 2) AND (x >= 2)';
SELECT * FROM rqtable_02131;

DROP ROW POLICY filter_02131_4 ON rqtable_02131;
SELECT 'R5: (x >= 2)';
SELECT * FROM rqtable_02131;

DROP ROW POLICY filter_02131_5 ON rqtable_02131;
SELECT 'None';
SELECT * FROM rqtable_02131;

DROP TABLE rqtable_02131;

SELECT 'Check system.query_log';
SYSTEM FLUSH LOGS;
SELECT query, used_row_policies FROM system.query_log WHERE current_database == currentDatabase() AND type == 'QueryStart' AND query_kind == 'Select' ORDER BY event_time_microseconds;
