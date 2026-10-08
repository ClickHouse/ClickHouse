SET enable_time_time64_type = 1;
SET input_format_csv_use_default_on_bad_values = 1;
SET input_format_parallel_parsing = 1;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0
(
    c0 Int,
    c1 Time,
    c2 Int128,
    c3 Int
) ENGINE = Memory;

INSERT INTO t0 (c2, c1, c3, c0) VALUES
    ( 1, '0:00:00', 1, 1),
    (-10011714060220656711, '0:00:00', 2, 1),
    ( 2, '0:00:00', 3, 2);

-- Write to server-side user_files (relative path resolves there)
INSERT INTO FUNCTION file(currentDatabase() || '_table_time_bug.csv', 'CSV',
    'c3 Int, c2 Int128, c1 Time, c0 Int')
SELECT c3, c2, c1, c0 FROM t0;

TRUNCATE TABLE t0;

-- Read it back from server-side using the file() table function
-- (NOT FROM INFILE, which is client-side)
INSERT INTO t0 (c0, c1, c2, c3)
SELECT c0, c1, c2, c3
FROM file(currentDatabase() || '_table_time_bug.csv', 'CSV',
          'c0 Int, c1 Time, c2 Int128, c3 Int');