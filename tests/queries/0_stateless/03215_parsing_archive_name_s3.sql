-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT _file, _path FROM s3(s3_conn, filename='::03215_archive.csv') ORDER BY (_file, _path);
SELECT _file, _path FROM s3(s3_conn, filename='test :: 03215_archive.csv') ORDER BY (_file, _path); -- { serverError S3_ERROR }
SELECT _file, _path FROM s3(s3_conn, filename='test::03215_archive.csv') ORDER BY (_file, _path);
SELECT _file, _path FROM s3(s3_conn, filename='test.zip::03215_archive.csv') ORDER BY (_file, _path) SETTINGS allow_archive_path_syntax=0;
