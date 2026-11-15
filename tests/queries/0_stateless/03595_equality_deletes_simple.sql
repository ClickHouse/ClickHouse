-- Tags: no-fasttest, no-parallel-replicas

SELECT sum(id) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id), count(name) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

-- Equality deletes on the `data` column, which is required in the table schema
-- but optional in the delete file schema (their nullability may legally differ).
SELECT '--- equality deletes with mismatched nullability ---';
-- The table's metadata declares `location` as `/out/warehouse/default/eq_deletes_required_table`,
-- so the queried path is kept as a component-aligned suffix of it. This branch resolves a data path
-- with `getProperFilePathFromMetadataInfo`, whose fast path needs `table_location` to end with the
-- queried path; `IcebergPathResolver`, which does not, is not in 26.3.
SELECT data FROM icebergS3(s3_conn, filename = 'default/eq_deletes_required_table') ORDER BY data;
