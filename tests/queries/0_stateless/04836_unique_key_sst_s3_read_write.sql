-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
--
-- UNIQUE KEY on S3: SST sidecar read/write through IDataPartStorage.
--
-- The SST reader/writer goes through IDataPartStorage::readFile/writeFile,
-- so it works on any disk type. This test verifies the round-trip on S3.
-- Before the refactor the reader opened the SST by local filesystem path
-- (getFullPath()), which fails on S3 - the file is a remote object.
SET allow_experimental_unique_key = 1;
DROP TABLE IF EXISTS uk_s3_rw;

CREATE TABLE uk_s3_rw (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS disk = 's3_disk', min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO uk_s3_rw VALUES (10, 'a'), (20, 'b'), (30, 'c');

-- DETACH + ATTACH: load-time validation reads the SST back from S3 through
-- IDataPartStorage.
DETACH TABLE uk_s3_rw;
ATTACH TABLE uk_s3_rw;

SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_s3_rw' AND active;

SELECT id, v FROM uk_s3_rw ORDER BY id;

-- ALTER MODIFY SETTING disk: migrate an existing UNIQUE KEY table to S3.
DROP TABLE uk_s3_rw;
CREATE TABLE uk_s3_rw (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO uk_s3_rw VALUES (10, 'a'), (20, 'b'), (30, 'c');

ALTER TABLE uk_s3_rw MODIFY SETTING disk = 's3_disk';

SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 'uk_s3_rw' AND active;

SELECT id, v FROM uk_s3_rw ORDER BY id;

DROP TABLE uk_s3_rw;
