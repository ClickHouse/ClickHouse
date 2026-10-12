-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- The fixture was written by ClickHouse 26.8 with PARTITION BY (icebergTruncate(10, i32), icebergTruncate(100, i64)).
-- Before 26.9 a negative source value got a partition value that is not a multiple of the width
-- (-1 -> -6 instead of -10), so the table cannot be produced by a current server.

DROP TABLE IF EXISTS t_legacy;
CREATE TABLE t_legacy ENGINE = IcebergS3(s3_conn, filename = 'iceberg_truncate_negative_legacy');

SELECT partition FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't_legacy' ORDER BY partition;

-- Each row: both pruning layers on, only per-file partition pruning, no partition pruning.
SELECT 'i32 = -1',
    (SELECT count() FROM t_legacy WHERE i32 = -1 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i32 = -1 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i32 = -1 SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i32 = -3',
    (SELECT count() FROM t_legacy WHERE i32 = -3 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i32 = -3 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i32 = -3 SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i32 = -12',
    (SELECT count() FROM t_legacy WHERE i32 = -12 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i32 = -12 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i32 = -12 SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i32 BETWEEN -5 AND -1',
    (SELECT count() FROM t_legacy WHERE i32 BETWEEN -5 AND -1 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i32 BETWEEN -5 AND -1 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i32 BETWEEN -5 AND -1 SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i32 IN (-1, -12)',
    (SELECT count() FROM t_legacy WHERE i32 IN (-1, -12) SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i32 IN (-1, -12) SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i32 IN (-1, -12) SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i64 = -1',
    (SELECT count() FROM t_legacy WHERE i64 = -1 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i64 = -1 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i64 = -1 SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i64 = -150',
    (SELECT count() FROM t_legacy WHERE i64 = -150 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i64 = -150 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i64 = -150 SETTINGS use_iceberg_partition_pruning = 0);
SELECT 'i32 = 23',
    (SELECT count() FROM t_legacy WHERE i32 = 23 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_legacy WHERE i32 = 23 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_legacy WHERE i32 = 23 SETTINGS use_iceberg_partition_pruning = 0);

-- A spec-valid key still skips every other manifest.
SELECT count() FROM t_legacy WHERE i32 = 23
SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1, log_comment = '05366_spec_value_probe';

-- Per-file partition pruning still skips a spec-valued entry, and a summary is not widened past it.
SELECT count() FROM t_legacy WHERE i32 = -1
SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0, log_comment = '05366_file_probe';
SELECT count() FROM t_legacy WHERE i32 = 33
SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1, log_comment = '05366_list_probe';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['IcebergPartitionPrunedManifestFiles']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05366_spec_value_probe';
SELECT ProfileEvents['IcebergPartitionPrunedFiles']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05366_file_probe';
SELECT ProfileEvents['IcebergPartitionPrunedManifestFiles']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05366_list_probe';

-- Mutations are refused on a table holding such a value. The predicates match no row, so even
-- without the refusal nothing would be written and the shared fixture stays untouched.
SET allow_insert_into_iceberg = 1;
ALTER TABLE t_legacy DELETE WHERE s = '05366 no such row'; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_legacy UPDATE s = 'x' WHERE s = '05366 no such row'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t_legacy SETTINGS use_iceberg_partition_pruning = 0;

DROP TABLE t_legacy;

-- A current server stores truncate(10, INT64_MIN) as a positive value that is not a multiple of 10
-- (the subtraction wraps). That is a spec value, so it must not be taken for a legacy one: the
-- mutation below is allowed.
DROP TABLE IF EXISTS t_wrap;
CREATE TABLE t_wrap (i64 Int64)
ENGINE = IcebergS3(s3_conn, filename = '05366_wrap/' || currentDatabase())
PARTITION BY icebergTruncate(10, i64);
INSERT INTO t_wrap VALUES (-9223372036854775808), (-1), (5);

SELECT partition FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't_wrap' ORDER BY partition;
SELECT 'i64 = INT64_MIN',
    (SELECT count() FROM t_wrap WHERE i64 = -9223372036854775808 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1),
    (SELECT count() FROM t_wrap WHERE i64 = -9223372036854775808 SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0),
    (SELECT count() FROM t_wrap WHERE i64 = -9223372036854775808 SETTINGS use_iceberg_partition_pruning = 0);

ALTER TABLE t_wrap DELETE WHERE i64 = 42;
SELECT count() FROM t_wrap SETTINGS use_iceberg_partition_pruning = 0;

DROP TABLE t_wrap;
