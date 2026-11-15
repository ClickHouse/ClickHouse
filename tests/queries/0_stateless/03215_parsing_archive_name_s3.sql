-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET s3_truncate_on_insert=1;

INSERT INTO FUNCTION s3(s3_conn, filename='::03215_archive.csv') SELECT 1;
SELECT _file, _path FROM s3(s3_conn, filename='::03215_archive.csv') ORDER BY (_file, _path);

SELECT _file, _path FROM s3(s3_conn, filename='test :: 03215_archive.csv') ORDER BY (_file, _path); -- { serverError S3_ERROR }

INSERT INTO FUNCTION s3(s3_conn, filename='test::03215_archive.csv') SELECT 1;
SELECT _file, _path FROM s3(s3_conn, filename='test::03215_archive.csv') ORDER BY (_file, _path);

INSERT INTO FUNCTION s3(s3_conn, filename='test.zip::03215_archive.csv') SETTINGS allow_archive_path_syntax=0 SELECT 1;
SELECT _file, _path FROM s3(s3_conn, filename='test.zip::03215_archive.csv') ORDER BY (_file, _path) SETTINGS allow_archive_path_syntax=0;
