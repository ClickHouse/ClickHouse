-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` and `AzureQueue` engines are not built in the fast test.
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

-- `source`: stored path `05366_queue/*`, local path `05366_other/*`.
CREATE TABLE tq9 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_other/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366'; -- { serverError METADATA_MISMATCH }

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

-- In `exclusive` mode each server reads its own path, so the paths may differ, but the bucket must be the same.
CREATE TABLE tq_x1 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_x1/*', NOSIGN, 'CSV') SETTINGS mode = 'exclusive', keeper_path = '/clickhouse/{database}/05366_exclusive';
CREATE TABLE tq_x2 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_x2/*', NOSIGN, 'CSV') SETTINGS mode = 'exclusive', keeper_path = '/clickhouse/{database}/05366_exclusive';
-- A different bucket is rejected.
CREATE TABLE tq_x3 (x String) ENGINE = S3Queue('http://localhost:11111/other/05366_x1/*', NOSIGN, 'CSV') SETTINGS mode = 'exclusive', keeper_path = '/clickhouse/{database}/05366_exclusive'; -- { serverError METADATA_MISMATCH }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'tq_x%';
DROP TABLE tq_x2; DROP TABLE tq_x1;

-- An `AzureQueue` and an `S3Queue` with the same endpoint, bucket and path read different storages.
CREATE TABLE tq_type_azure (x String) ENGINE = AzureQueue('http://localhost:11111', 'test', '05366_type/*', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_type';
CREATE TABLE tq_type_s3 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_type/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_type'; -- { serverError METADATA_MISMATCH }
DROP TABLE tq_type_azure;

-- A URL with user info is refused, because the user info would be saved in keeper as a part of the source.
CREATE TABLE tq_u (x String) ENGINE = S3Queue('http://user:secret@localhost:11111/test/05366_u/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_user_info'; -- { serverError BAD_ARGUMENTS }

-- Azure forms that have a source: a scheme in upper case, and an `abfss` URL with a separate SAS.
CREATE TABLE tq_az_upper (x String) ENGINE = AzureQueue('HTTP://localhost:11111', 'test', '05366_upper/*', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_upper';
CREATE TABLE tq_az_sas (x String) ENGINE = AzureQueue('abfss://cont@acc.dfs.core.windows.net/05366_sas/*.csv', 'sv=1&sig=2') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_sas';
SELECT JSONExtractString(value, 'source_endpoint'), JSONExtractString(value, 'source_bucket') FROM system.zookeeper WHERE path IN ('/clickhouse/' || currentDatabase() || '/05366_upper', '/clickhouse/' || currentDatabase() || '/05366_sas') AND name = 'metadata' ORDER BY path;
DROP TABLE tq_az_upper;
DROP TABLE tq_az_sas;

-- A table attached from its saved definition must not fail when it differs from keeper: it uses `after_processing`
-- from keeper and the server logs a warning. `send_logs_level` keeps these warnings out of the test output.
SET send_logs_level = 'error';

CREATE TABLE tq_a (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_shared/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_shared', processing_threads_num = 1;
CREATE TABLE tq_b (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_shared/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_shared', processing_threads_num = 1;
SELECT JSONExtractString(value, 'source_type'), JSONExtractString(value, 'source_endpoint'), JSONExtractString(value, 'source_bucket'), JSONExtractString(value, 'source_path_pattern') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';

-- After the `ALTER` of `tq_a`, `tq_b` still has `keep` in its definition, but is attached with `delete` from keeper.
ALTER TABLE tq_a MODIFY SETTING after_processing = 'delete';
DETACH TABLE tq_b; ATTACH TABLE tq_b;
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table IN ('tq_a', 'tq_b') AND name = 'after_processing' ORDER BY table;

-- Set `after_processing` to `keep` directly in keeper, as another server would. The in-memory metadata of this server
-- still has `delete`, and `tq_b` must still attach.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '"after_processing":"delete"', '"after_processing":"keep"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
DETACH TABLE tq_b; ATTACH TABLE tq_b;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tq_b';
-- An `ALTER` of another setting must not write the outdated `delete` of this server back to keeper.
ALTER TABLE tq_a MODIFY SETTING polling_max_timeout_ms = 2000;
SELECT JSONExtractString(value, 'after_processing') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';

-- Change the source in keeper: `tq_b` must still attach, with a warning.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '05366_shared', '05366_other') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
DETACH TABLE tq_b; ATTACH TABLE tq_b;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tq_b';

-- Remove `source` from keeper, as in metadata of an older version: the first attached table writes it back.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceRegexpAll(value, ',"source_[a-z_]+":("[^"]*"|[0-9]+)', '') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
DETACH TABLE tq_a; DETACH TABLE tq_b; ATTACH TABLE tq_b; ATTACH TABLE tq_a;
SELECT JSONExtractString(value, 'source_endpoint') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';

DROP TABLE tq_b;
DROP TABLE tq_a;
