-- Tags: no-fasttest
-- Requires `IcebergLocal` (USE_AVRO).
-- Regression: https://github.com/ClickHouse/ClickHouse/issues/120165

SET allow_insert_into_iceberg = 1;
SET allow_experimental_iceberg_compaction = 1;
SET use_iceberg_metadata_files_cache = 0;

-- Resolve relative user-files paths against the server's data directory.
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05291_iceberg_optimize_explicit_metadata/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (id Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2;

INSERT INTO t VALUES (1), (2); -- v2
DELETE FROM t WHERE id = 1;    -- v3, with a position-delete file
INSERT INTO t VALUES (3);      -- v4, committed before compaction
SELECT id FROM t ORDER BY id;

CREATE TABLE pinned
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json';
SELECT id FROM pinned ORDER BY id;

OPTIMIZE TABLE pinned; -- { serverError BAD_ARGUMENTS }

-- A fresh, unpinned reader must still see the row committed in v4.
SELECT id FROM icebergLocal((SELECT path FROM iceberg_path), 'Parquet') ORDER BY id;

DROP TABLE pinned SYNC;
DROP TABLE t SYNC;
DROP TABLE iceberg_path;
