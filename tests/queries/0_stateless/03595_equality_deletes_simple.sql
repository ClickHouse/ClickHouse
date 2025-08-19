-- Tags: no-fasttest

SELECT sum(id), count(name) FROM icebergS3(s3_conn, file_name='equality_deletes_table');
