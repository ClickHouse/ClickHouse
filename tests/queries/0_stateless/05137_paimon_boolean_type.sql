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

SELECT countIf(f_boolean_nn), countIf(NOT f_boolean_nn), countIf(f_boolean IS NULL)
FROM paimonS3(s3_conn, filename = 'paimon_all_types');

-- A boolean literal against a plain `Bool` column.
SELECT count()
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean_nn = true;

-- `f_boolean` is a partition key of this table, so a boolean literal compared against it is what
-- drives `PartitionPruner` through a `Bool` partition key. Pruning only decides which files are
-- read, so the count must come out the same either way; a pruner that mishandles the `Bool`
-- literal drops the matching partition and returns a smaller count with pruning on.
SELECT count()
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean = false
SETTINGS use_paimon_partition_pruning = 0;

SELECT count()
FROM paimonS3(s3_conn, filename = 'paimon_all_types')
WHERE f_boolean = false
SETTINGS use_paimon_partition_pruning = 1;
