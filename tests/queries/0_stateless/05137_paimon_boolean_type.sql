-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: Depends on AWS
-- Tag no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas
-- the query runs in, and the coordinator does not collect all ProfileEvents values.

-- A Paimon `BOOLEAN` column must be reported as `Bool`, like in Iceberg and Delta Lake, and not as
-- `Int8`, which rendered the values as `0`/`1` and made it indistinguishable from a Paimon `TINYINT`.
-- https://github.com/ClickHouse/ClickHouse/issues/119272

SELECT toTypeName(f_boolean), toTypeName(f_boolean_nn), toTypeName(f_tinyint)
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
LIMIT 1;

SELECT '=== toString';

SELECT DISTINCT toString(f_boolean), toString(f_boolean_nn)
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
ORDER BY 1, 2;

SELECT '=== filtering by a boolean literal';

SELECT countIf(f_boolean_nn), countIf(NOT f_boolean_nn), countIf(f_boolean IS NULL)
FROM paimonS3(s3_conn, filename = 'paimon_all_types');

-- A boolean literal against a plain `Bool` column.
SELECT count()
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean_nn = true;

SELECT '=== partition pruning by a boolean literal';

-- `f_boolean` is a partition key of this table, so a boolean literal compared against it is what
-- drives `PartitionPruner` through a `Bool` partition key. Pruning only decides which files are
-- read, so both queries must return the same rows, while the number of files read must drop from
-- every file of the table to the matching partitions only. A pruner that does not recognize the
-- `Bool` literal returns the same rows, but keeps reading every file, so the rows alone prove
-- nothing and `EngineFileLikeReadFiles` is asserted below.
SELECT f_int_nn
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean = false
ORDER BY f_int_nn
SETTINGS use_paimon_partition_pruning = 0, log_comment = '05137_pruning_off';

SELECT f_int_nn
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean = false
ORDER BY f_int_nn
SETTINGS use_paimon_partition_pruning = 1, log_comment = '05137_pruning_on';

SYSTEM FLUSH LOGS query_log;

SELECT 'files read without pruning', ProfileEvents['EngineFileLikeReadFiles']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05137_pruning_off'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT 'files read with pruning', ProfileEvents['EngineFileLikeReadFiles']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05137_pruning_on'
ORDER BY event_time_microseconds DESC
LIMIT 1;
