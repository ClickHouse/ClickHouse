-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Iceberg leaves NaN out of the bounds of a float column and counts it in `nan_value_counts`, and
-- a NaN satisfies `NOT (f <= 3)` and `f != 3`. Min/max pruning must not drop a file for such a
-- predicate unless the count says the file holds no NaN. The fixtures are written by Spark:
-- - iceberg_nan_float_bounds (`f` double, `g` float): file 1 holds 3, 3, NaN (bounds [3, 3], NaN
--   count 1), file 2 holds 100, 200 (bounds [100, 200], NaN count 0), file 3 holds one NaN (no bounds).
-- - iceberg_nan_float_no_nan_counts: 3, 3, NaN and 10, 20, with the manifests rewritten by
--   `OPTIMIZE TABLE ... MANIFEST`, which keeps the bounds and drops `nan_value_counts`.
-- - iceberg_nan_float_partition: identity partitions on `f` with the values 1.0, NaN and 100.0.

SELECT 'bounds: a comparison NaN cannot satisfy prunes file 1';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_bounds') WHERE f > 3 SETTINGS log_comment = 'q01';
SELECT 'bounds: the NaN row of file 1 matches a negation';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_bounds') WHERE NOT (f <= 3) SETTINGS log_comment = 'q02';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_bounds') WHERE f != 3 SETTINGS log_comment = 'q03';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_bounds') WHERE NOT (g <= 3) SETTINGS log_comment = 'q04';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_bounds') WHERE g != 3 SETTINGS log_comment = 'q05';
SELECT 'bounds: a NaN count of 0 still prunes file 2 for a negation';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_bounds') WHERE NOT (f >= 100) SETTINGS log_comment = 'q06';

SELECT 'no NaN counts: a comparison NaN cannot satisfy prunes the file with bounds [3, 3]';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_no_nan_counts') WHERE f > 3 SETTINGS log_comment = 'q07';
SELECT 'no NaN counts: a negation prunes nothing';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_no_nan_counts') WHERE NOT (f <= 20) SETTINGS log_comment = 'q08';

SELECT 'partitions: the NaN partition is kept';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_partition') WHERE NOT (f <= 3) SETTINGS log_comment = 'q09';
SELECT count() FROM icebergS3(s3_conn, filename = 'iceberg_nan_float_partition') WHERE f > 3 SETTINGS log_comment = 'q10';

SELECT 'pruned files per query: min/max, partition';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['IcebergMinMaxIndexPrunedFiles'], ProfileEvents['IcebergPartitionPrunedFiles']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'q__'
ORDER BY log_comment;
