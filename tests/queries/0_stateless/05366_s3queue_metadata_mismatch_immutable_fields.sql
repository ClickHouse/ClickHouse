-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` engine is not built in the fast test.
-- zookeeper: the immutable part of the queue metadata lives under `keeper_path` in keeper.

-- A queue table stores the settings listed in `ObjectStorageQueueTableMetadata::isStoredInKeeper`
-- under its `keeper_path` (docs/reference/engines/table-engines/integrations/s3queue.mdx, section
-- `keeper_path`). A second table pointed at the same path must agree with what is already stored
-- there, so each of the immutable fields below is rejected with `METADATA_MISMATCH` on its own.
-- The object storage is never contacted: the metadata check happens while the engine is built.

DROP TABLE IF EXISTS tq_base;
DROP TABLE IF EXISTS tq_bucket_base;

CREATE TABLE tq_base (x String)
ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366', processing_threads_num = 1,
         polling_min_timeout_ms = 1, polling_max_timeout_ms = 1, polling_backoff_ms = 0;

-- `after_processing`: stored as 'keep', local 'delete'.
CREATE TABLE tq1 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         after_processing = 'delete'; -- { serverError METADATA_MISMATCH }

-- `bucketing_mode`: stored as 'path', local 'partition'. A 'partition' bucketing mode hashes the
-- partition key instead of the path, so `bucketing_mode = 'partition'` is rejected outright unless a
-- partitioning mode is set too. To vary `bucketing_mode` alone, a second keeper path already stores
-- `partitioning_mode = 'regex'`, which needs no experimental setting.
CREATE TABLE tq_bucket_base (x String)
ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_bucket',
         partitioning_mode = 'regex', partition_regex = '(?P<part>.+)/', partition_component = 'part',
         processing_threads_num = 1,
         polling_min_timeout_ms = 1, polling_max_timeout_ms = 1, polling_backoff_ms = 0;

CREATE TABLE tq2 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_bucket',
         partitioning_mode = 'regex', partition_regex = '(?P<part>.+)/', partition_component = 'part',
         bucketing_mode = 'partition'; -- { serverError METADATA_MISMATCH }

-- `partitioning_mode`: stored as 'none', local 'regex'. The stored `partition_regex` and
-- `partition_component` differ too, but the partitioning mode is compared first.
CREATE TABLE tq3 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         partitioning_mode = 'regex', partition_regex = '(?P<part>.+)/',
         partition_component = 'part'; -- { serverError METADATA_MISMATCH }

-- `partition_regex`: stored empty, local set.
CREATE TABLE tq4 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         partition_regex = '(?P<part>.+)'; -- { serverError METADATA_MISMATCH }

-- `partition_component`: stored empty, local set.
CREATE TABLE tq5 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         partition_component = 'part'; -- { serverError METADATA_MISMATCH }

-- `tracked_files_limit`: stored as the default 1000, local 7.
CREATE TABLE tq6 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         tracked_files_limit = 7; -- { serverError METADATA_MISMATCH }

-- `tracked_files_ttl_sec`: stored as the default 0, local 60.
CREATE TABLE tq7 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         tracked_file_ttl_sec = 60; -- { serverError METADATA_MISMATCH }

-- `last_processed_path`: stored empty, local set.
CREATE TABLE tq8 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366',
         last_processed_path = 'a.csv'; -- { serverError METADATA_MISMATCH }

-- Every mismatching table was rejected, so only the two base tables exist.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'tq%';

-- The check is selective: a table that agrees on all of the immutable fields joins the same path.
CREATE TABLE tq_same (x String)
ENGINE = S3Queue('http://localhost:11111/test/05366_queue/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366', processing_threads_num = 1,
         polling_min_timeout_ms = 1, polling_max_timeout_ms = 1, polling_backoff_ms = 0;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'tq%';

DROP TABLE tq_same;
DROP TABLE tq_bucket_base;
DROP TABLE tq_base;
