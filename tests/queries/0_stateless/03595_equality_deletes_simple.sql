-- Tags: no-fasttest

SELECT sum(id), count(name) FROM icebergS3(s3_conn, file_name = 'deletes_db/eq_deletes_table');
