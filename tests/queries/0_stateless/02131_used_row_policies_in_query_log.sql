DROP TABLE IF EXISTS _02131_rqtable;
CREATE TABLE _02131_rqtable (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO _02131_rqtable VALUES (1), (2), (3), (4);

DROP ROW POLICY IF EXISTS _02131_filter_1 ON _02131_rqtable;
DROP ROW POLICY IF EXISTS _02131_filter_2 ON _02131_rqtable;
DROP ROW POLICY IF EXISTS _02131_filter_3 ON _02131_rqtable;
DROP ROW POLICY IF EXISTS _02131_filter_4 ON _02131_rqtable;
DROP ROW POLICY IF EXISTS _02131_filter_5 ON _02131_rqtable;

SELECT 'None';
SELECT * FROM _02131_rqtable;

CREATE ROW POLICY _02131_filter_1 ON _02131_rqtable USING x=1 AS permissive TO ALL;
SELECT 'R1: x == 1';
SELECT * FROM _02131_rqtable;

CREATE ROW POLICY _02131_filter_2 ON _02131_rqtable USING x=2 AS permissive TO ALL;
SELECT 'R1, R2: (x == 1) OR (x == 2)';
SELECT * FROM _02131_rqtable;

CREATE ROW POLICY _02131_filter_3 ON _02131_rqtable USING x=3 AS permissive TO ALL;
SELECT 'R1, R2, R3: (x == 1) OR (x == 2) OR (x == 3)';
SELECT * FROM _02131_rqtable;

CREATE ROW POLICY _02131_filter_4 ON _02131_rqtable USING x<=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2)';
SELECT * FROM _02131_rqtable;

CREATE ROW POLICY _02131_filter_5 ON _02131_rqtable USING x>=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4, R5: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rqtable;

DROP ROW POLICY _02131_filter_1 ON _02131_rqtable;
SELECT 'R2, R3, R4, R5: ((x == 2) OR (x == 3)) AND (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rqtable;

DROP ROW POLICY _02131_filter_2 ON _02131_rqtable;
SELECT 'R3, R4, R5: (x == 3) AND (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rqtable;

DROP ROW POLICY _02131_filter_3 ON _02131_rqtable;
SELECT 'R4, R5: (x <= 2) AND (x >= 2)';
SELECT * FROM _02131_rqtable;

DROP ROW POLICY _02131_filter_4 ON _02131_rqtable;
SELECT 'R5: (x >= 2)';
SELECT * FROM _02131_rqtable;

DROP ROW POLICY _02131_filter_5 ON _02131_rqtable;
SELECT 'None';
SELECT * FROM _02131_rqtable;

DROP TABLE _02131_rqtable;

SELECT 'Check system.query_log';
SYSTEM FLUSH LOGS;
SELECT query, used_row_policies FROM system.query_log WHERE current_database == currentDatabase() AND type == 'QueryStart' AND query_kind == 'Select' ORDER BY event_time_microseconds;
