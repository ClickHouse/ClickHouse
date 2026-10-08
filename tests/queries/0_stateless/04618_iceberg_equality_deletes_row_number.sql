-- Tags: no-fasttest, no-parallel-replicas
-- `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`. The explicit
-- `PREWHERE` below throws `ILLEGAL_PREWHERE` once the storage is wrapped in
-- `StorageObjectStorageCluster`, which does not override `supportsPrewhere`.

-- `id - _row_number` is asserted as an exact value per data file, not merely as a constant: a mask
-- that replaced rather than composed would keep it constant while shifting it by the number of
-- rows deleted before each surviving row. The expected offsets are the ones the raw Parquet data
-- files carry, i.e. they are independent of the engine read path.
SELECT '--- equality + position deletes: exact id - _row_number offset per data file ---';
SELECT splitByChar('/', _path)[-1] AS file, groupUniqArray(id - _row_number) AS offsets
FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')
GROUP BY file
ORDER BY file;

-- This fixture is same-schema Parquet, so `PREWHERE` stays inside the reader (it is not stripped
-- into a fallback filter); the reader-side mask is then composed with the equality-delete mask.
SELECT '--- the same with reader-side PREWHERE composed with the equality-delete mask ---';
SELECT splitByChar('/', _path)[-1] AS file, groupUniqArray(id - _row_number) AS offsets
FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')
PREWHERE id % 7 = 3
GROUP BY file
ORDER BY file
SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT '--- equality deletes only: exact physical row numbers ---';
SELECT data, _row_number
FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_required_table')
ORDER BY data;
