-- Tags: no-fasttest, no-parallel-replicas

SELECT sum(id) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id), count(name) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
