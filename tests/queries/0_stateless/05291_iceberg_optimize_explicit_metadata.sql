-- Tags: no-fasttest
-- Requires `IcebergLocal` (USE_AVRO).
-- Regression: https://github.com/ClickHouse/ClickHouse/issues/120165

SET allow_insert_into_iceberg = 1;
SET allow_experimental_iceberg_compaction = 0;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 0;
SET async_insert = 0;

-- Resolve relative user-files paths against the server's data directory.
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05291_iceberg_optimize_explicit_metadata/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

-- Disable autonomous compaction while constructing and reading the fixture.
CREATE TABLE t (id Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2, allow_experimental_iceberg_compaction = 0;

INSERT INTO t VALUES (1), (2); -- v2
DELETE FROM t WHERE id = 1;    -- v3, with a position-delete file
INSERT INTO t VALUES (3);      -- v4, committed before compaction
SELECT id FROM t ORDER BY id;

OPTIMIZE TABLE t SETTINGS allow_experimental_iceberg_compaction = 1, iceberg_snapshot_id = 1; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't';
SELECT id FROM t ORDER BY id;

OPTIMIZE TABLE t SETTINGS allow_experimental_iceberg_compaction = 1, iceberg_timestamp_ms = 9223372036854775807; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't';
SELECT id FROM t ORDER BY id;

CREATE TABLE pinned
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json',
         allow_experimental_iceberg_compaction = 0;
SELECT id FROM pinned ORDER BY id;

OPTIMIZE TABLE pinned SETTINGS allow_experimental_iceberg_compaction = 1; -- { serverError NOT_IMPLEMENTED }

-- Both the latest committed state and the historical metadata must survive.
SELECT id
FROM icebergLocal((SELECT path FROM iceberg_path), 'Parquet')
ORDER BY id
SETTINGS allow_experimental_iceberg_compaction = 0;
SELECT id FROM pinned ORDER BY id;

-- A lagging version hint is another way to select the same stale metadata.
INSERT INTO FUNCTION file(
    concat((SELECT path FROM iceberg_path), 'metadata/version-hint.text'),
    'RawBLOB',
    'hint String')
SELECT '3'
SETTINGS engine_file_truncate_on_insert = 1;

CREATE TABLE hinted
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_use_version_hint = 1,
         allow_experimental_iceberg_compaction = 0;
SELECT id FROM hinted ORDER BY id;

-- OSS rejects the stale hint with (NOT_IMPLEMENTED); the cloud fails with (BAD_ARGUMENTS) at a later stage
OPTIMIZE TABLE hinted SETTINGS allow_experimental_iceberg_compaction = 1; -- { serverError NOT_IMPLEMENTED, BAD_ARGUMENTS }

SELECT id
FROM icebergLocal((SELECT path FROM iceberg_path), 'Parquet')
ORDER BY id
SETTINGS allow_experimental_iceberg_compaction = 0;
SELECT id FROM hinted ORDER BY id;

DROP TABLE hinted SYNC;
DROP TABLE pinned SYNC;
SET iceberg_delete_data_on_drop = 1;
DROP TABLE t SYNC;
DROP TABLE iceberg_path;
