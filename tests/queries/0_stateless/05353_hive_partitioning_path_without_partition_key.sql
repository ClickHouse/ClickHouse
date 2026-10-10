-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- Some paths of a glob do not carry the Hive partition column `a`: a filter or a read that needs it fails
-- with INCORRECT_DATA, a filter that does not need it works.

SET use_hive_partitioning = 1;

-- `url` addresses are filtered before any connection is made.
SELECT count() FROM url('http://localhost:11111/test/{a=1/x,b/x}.csv', CSV, 'x UInt8, a UInt8') WHERE a = 1; -- { serverError INCORRECT_DATA }
SELECT count() FROM url('http://localhost:11111/test/{a=1/x,b/x}.csv', CSV, 'x UInt8') WHERE a = 1; -- { serverError INCORRECT_DATA }
SELECT count() FROM url('http://localhost:11111/test/{a=1/x,b/x}.csv', CSV, 'x UInt8') WHERE _file = 'none.csv';

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05353/a=1/data.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 1 AS x;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05353/b/data.csv', format = CSV) SETTINGS s3_truncate_on_insert = 1 SELECT 2 AS x;

SELECT x FROM s3(s3_conn, filename = currentDatabase() || '/05353/*/data.csv', format = CSV, structure = 'x UInt8') WHERE _file = 'data.csv' ORDER BY x;
SELECT x, a FROM s3(s3_conn, filename = currentDatabase() || '/05353/*/data.csv', format = CSV, structure = 'x UInt8') ORDER BY x; -- { serverError INCORRECT_DATA }
SELECT x FROM s3(s3_conn, filename = currentDatabase() || '/05353/*/data.csv', format = CSV, structure = 'x UInt8') WHERE a = 1; -- { serverError INCORRECT_DATA }

-- A table created with `use_hive_partitioning = 1` still reads its Hive column from the path when the setting is off.
CREATE TABLE t_05353 (x UInt8) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05353/a=1/data.csv', format = CSV);
SELECT x, a FROM t_05353 SETTINGS use_hive_partitioning = 0;
SELECT x FROM t_05353 WHERE a = 1 SETTINGS use_hive_partitioning = 0;
DROP TABLE t_05353;
