-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3
-- Pruning files and parts by virtual columns must not evaluate a branch that a short-circuit guard excludes.

INSERT INTO FUNCTION file(currentDatabase() || '_05353/events_2024-01-01.csv', CSV, 'a Int32, b String') SELECT 1, 'a' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05353/readme.csv', CSV, 'a Int32, b String') SELECT 2, 'b' SETTINGS engine_file_truncate_on_insert = 1;

SELECT count() FROM file(currentDatabase() || '_05353/*.csv', CSV, 'a Int32, b String') WHERE _file LIKE 'events_%' AND toDate(substring(_file, 8, 10)) >= '2024-01-01';
SELECT count() FROM file(currentDatabase() || '_05353/*.csv', CSV, 'a Int32, b String') WHERE if(_file LIKE 'events_%', toDate(substring(_file, 8, 10)) >= '2024-01-01', 0);
SELECT count() FROM file(currentDatabase() || '_05353/*.csv', CSV, 'a Int32, b String') WHERE _file = 'readme.csv' OR toDate(substring(_file, 8, 10)) >= '2024-01-01';
SELECT count() FROM file(currentDatabase() || '_05353/*.csv', CSV, 'a Int32, b String') WHERE _file LIKE 'events_%' AND toDate(substring(_file, 8, 10)) >= '2024-01-01' SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError CANNOT_PARSE_DATE }

DROP TABLE IF EXISTS t_05353;
CREATE TABLE t_05353 (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05353 VALUES (1);
-- The only part is `all_1_1_0`: the guard holds and `length(_part) - 9` is 0.
SELECT count() FROM t_05353 WHERE _part LIKE 'all\_%' OR intDiv(10, length(_part) - 9) > 0;
SELECT sum(x) FROM t_05353 WHERE if(_part LIKE 'all\_%', 1, intDiv(10, length(_part) - 9) > 0);
SELECT sum(x) FROM t_05353 WHERE _part LIKE 'all\_%' OR intDiv(10, length(_part) - 9) > 0 SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }
DROP TABLE t_05353;

INSERT INTO FUNCTION s3(s3_conn, filename = concat(currentDatabase(), '_05353/events_2024-01-01.csv'), format = CSV, structure = 'a Int32, b String') SELECT 1, 'a' SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = concat(currentDatabase(), '_05353/readme.csv'), format = CSV, structure = 'a Int32, b String') SELECT 2, 'b' SETTINGS s3_truncate_on_insert = 1;
SELECT count() FROM s3(s3_conn, filename = concat(currentDatabase(), '_05353/*.csv'), format = CSV, structure = 'a Int32, b String') WHERE _file LIKE 'events_%' AND toDate(substring(_file, 8, 10)) >= '2024-01-01';
SELECT count() FROM s3(s3_conn, filename = concat(currentDatabase(), '_05353/{events_2024-01-01,readme}.csv'), format = CSV, structure = 'a Int32, b String') WHERE _file LIKE 'events_%' AND toDate(substring(_file, 8, 10)) >= '2024-01-01';
SELECT count() FROM s3(s3_conn, filename = concat(currentDatabase(), '_05353/readme.csv'), format = CSV, structure = 'a Int32, b String') WHERE _file LIKE 'events_%' AND toDate(substring(_file, 8, 10)) >= '2024-01-01';

-- `_path` of this URL is '/'.
SELECT count() FROM url('http://localhost:8123/?query=SELECT%201', TSV, 'x UInt64') WHERE _path = '/' OR intDiv(10, length(_path) - 1) > 0;
