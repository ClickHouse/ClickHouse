-- Tags: no-fasttest, no-parallel-replicas
-- - no-fasttest: reads Iceberg tables from Minio

-- Regression for the `_row_number` virtual column on Iceberg tables with equality deletes.
-- Equality deletes used to be applied by a plain `FilterTransform`, which does not maintain
-- `ChunkInfoRowNumbers`, so `_row_number` produced wrong values (or an exception). They now use
-- `RowNumbersPreservingFilterTransform`, so `_row_number` stays a correct physical row number.

-- Selecting `_row_number` must not throw and must not change the data.
SELECT sum(id) FROM (SELECT id, _row_number FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table'));

-- Every surviving row has a known physical row number (not NULL).
SELECT countIf(_row_number IS NULL) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

-- After equality deletes, each surviving row keeps a distinct (file, physical row number) identity.
SELECT count() = uniqExact((_file, _row_number)) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
