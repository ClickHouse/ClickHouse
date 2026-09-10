-- Tags: no-fasttest, no-parallel-replicas

SELECT sum(id) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id), count(name) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

-- Equality deletes on the `data` column, which is required in the table schema
-- but optional in the delete file schema (their nullability may legally differ).
SELECT '--- equality deletes with mismatched nullability ---';
SELECT data FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_required_table') ORDER BY data;
