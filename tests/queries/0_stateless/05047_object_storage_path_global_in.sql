-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- The set of a `GLOBAL IN` cannot be built while the listing prefilter is prepared, so the prefilter
-- must drop the atom instead of evaluating it against a not-ready set. Only the non-glob listing
-- prefilter is affected: the glob iterator filters lazily, once the set is already built.

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV) SELECT 1;

SELECT count() FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT _path FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64'));

SELECT count() FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL NOT IN (SELECT 'not_a_path');

SELECT count() FROM s3(s3_conn, filename = '05047_global_in/file1.csv', format = CSV, structure = 'x UInt64')
WHERE _file GLOBAL IN (SELECT 'file1.csv') AND x = 1;
