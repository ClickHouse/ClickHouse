-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- The `GLOBAL IN` set is not built yet when the listing prefilter runs, so the prefilter has to drop
-- it. Only the non-glob prefilter is affected - the glob iterator filters later, once the set is there.

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV) SELECT 1;

SELECT count() FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT _path FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64'));

SELECT count() FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL NOT IN (SELECT 'not_a_path');

SELECT count() FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64')
WHERE _file GLOBAL IN (SELECT 'file1.csv') AND x = 1;
