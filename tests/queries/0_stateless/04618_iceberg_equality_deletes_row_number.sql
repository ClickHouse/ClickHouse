-- Tags: no-fasttest, no-parallel-replicas
-- `no-parallel-replicas`: `StorageObjectStorageCluster` does not delegate `supportsPrewhere`
-- to its underlying configuration, so the fallback filter path below is not reached.

SELECT '--- equality + position deletes: id - _row_number is constant within each data file ---';
SELECT uniqExact(id - _row_number) AS offsets_in_file
FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')
GROUP BY _path
ORDER BY offsets_in_file;

SELECT '--- the same under PREWHERE, which adds the fallback filter on top of the notIn filter ---';
SELECT uniqExact(id - _row_number) AS offsets_in_file
FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')
PREWHERE id % 7 = 3
GROUP BY _path
ORDER BY offsets_in_file
SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT '--- equality deletes only: exact physical row numbers ---';
SELECT data, _row_number
FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_required_table')
ORDER BY data;
