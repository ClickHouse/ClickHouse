-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

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

-- `f_boolean` is also a partition key of this table, so this covers `Bool` as a partition key type
-- for the `PartitionPruner`; the result must not depend on whether pruning is used.
SELECT countIf(f_boolean_nn), countIf(NOT f_boolean_nn), countIf(f_boolean IS NULL)
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
SETTINGS use_paimon_partition_pruning = 0;

SELECT countIf(f_boolean_nn), countIf(NOT f_boolean_nn), countIf(f_boolean IS NULL)
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
SETTINGS use_paimon_partition_pruning = 1;

SELECT count()
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean_nn = true
SETTINGS use_paimon_partition_pruning = 0;

SELECT count()
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean_nn = true
SETTINGS use_paimon_partition_pruning = 1;
