INSERT INTO FUNCTION file(currentDatabase() || '_04771.csv.gz', 'CSV', 'x UInt64', 'gzip')
SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04771.csv', 'CSV', 'x UInt64', 'none')
SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04771.CSV.GZ', 'CSV', 'x UInt64', 'gzip')
SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'C1 auto differs from none on .gz';
SELECT (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'auto'))
    != (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'none'));

SELECT 'C2 GZIP codec name';
SELECT count() FROM file(currentDatabase() || '_04771.csv.gz', 'CSV', 'x UInt64', 'GZIP');

SELECT 'C3 unknown method still throws and echoes the raw spelling';
SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv', 'RawBLOB', 'x String', 'BOGUS'); -- { serverError NOT_IMPLEMENTED }
SYSTEM FLUSH LOGS query_log;
SELECT countIf(position(exception, 'BOGUS') > 0) > 0, countIf(position(exception, 'bogus') > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND exception_code = 48 AND event_date >= yesterday();

SELECT 'C4 uppercase file extension autodetects';
SELECT (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.CSV.GZ', 'RawBLOB', 'x String'))
    == (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'gzip'));

SELECT 'C5 empty hint autodetects';
SELECT (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String'))
    == (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'auto'));

SELECT 'W1 AUTO on .gz equals auto';
SELECT (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'AUTO'))
    == (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'auto'));

SELECT 'W2 NONE on .gz equals none';
SELECT (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'NONE'))
    == (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'none'));

SELECT 'W3 Auto on .gz equals auto';
SELECT (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'Auto'))
    == (SELECT sum(length(x)) FROM file(currentDatabase() || '_04771.csv.gz', 'RawBLOB', 'x String', 'auto'));

SELECT 'W4 None on plain file equals none';
SELECT (SELECT count() FROM file(currentDatabase() || '_04771.csv', 'CSV', 'x UInt64', 'None'))
    == (SELECT count() FROM file(currentDatabase() || '_04771.csv', 'CSV', 'x UInt64', 'none'));

SELECT 'W5 AUTO on the write path';
INSERT INTO FUNCTION file(currentDatabase() || '_04771_w.csv.gz', 'CSV', 'x UInt64', 'AUTO')
SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;
SELECT count() FROM file(currentDatabase() || '_04771_w.csv.gz', 'CSV', 'x UInt64', 'auto');
