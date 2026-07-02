-- Tags: no-fasttest
-- Tag no-fasttest: requires S3 storage support (USE_AWS_S3) which is disabled in fast test build.
-- Test: exercises the early-bail `NOT_IMPLEMENTED` paths in `StorageObjectStorage::write` and
--       `StorageObjectStorage::truncate` when the S3 path uses archive syntax (e.g. `*.zip :: file`).
-- Covers:
--   src/Storages/ObjectStorage/StorageObjectStorage.cpp:568 — INSERT throws "Write into archive is not supported"
--   src/Storages/ObjectStorage/StorageObjectStorage.cpp:633 — TRUNCATE throws "Table cannot be truncated"
-- The archive check happens BEFORE any network round-trip, so an unreachable host is fine.

-- INSERT INTO FUNCTION s3() with archive syntax must throw NOT_IMPLEMENTED
INSERT INTO FUNCTION s3('http://nonexistent.invalid/bkt/test.zip :: file.csv', 'CSV', 'a UInt32') VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- Same for .tar archive extension
INSERT INTO FUNCTION s3('http://nonexistent.invalid/bkt/test.tar :: file.csv', 'CSV', 'a UInt32') VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- TRUNCATE on a table backed by S3 archive must throw NOT_IMPLEMENTED.
DROP TABLE IF EXISTS t_arch_truncate;
CREATE TABLE t_arch_truncate (a UInt32)
    ENGINE = S3('http://nonexistent.invalid/bkt/test.zip :: file.csv', 'CSV');
TRUNCATE TABLE t_arch_truncate; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_arch_truncate;
