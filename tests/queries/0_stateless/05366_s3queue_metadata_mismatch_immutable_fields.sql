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

-- `source`: bucket `test0` with path `5366_queue/*` is another source than bucket `test` with path `05366_queue/*`.
CREATE TABLE tq10 (x String) ENGINE = S3Queue('http://localhost:11111/test0/5366_queue/*', NOSIGN, 'CSV')
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

-- A URL with user info is refused, because a hash of the user info would be saved in keeper as a part of the source.
CREATE TABLE tq_u (x String) ENGINE = S3Queue('http://user:secret@localhost:11111/test/05366_u/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_user_info'; -- { serverError BAD_ARGUMENTS }
-- Paths that differ only in a part like `&sig=` are different sources.
CREATE TABLE tq_sig (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_root&sig=a/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_sig';
CREATE TABLE tq_sig_z (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_root&sig=z/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_sig'; -- { serverError METADATA_MISMATCH }
DROP TABLE tq_sig;

-- Azure forms that have a source: a scheme in upper case, and an `abfss` URL with a separate SAS.
CREATE TABLE tq_az_upper (x String) ENGINE = AzureQueue('HTTP://localhost:11111', 'test', '05366_upper/*', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_upper';
CREATE TABLE tq_az_sas (x String) ENGINE = AzureQueue('abfss://cont@acc.dfs.core.windows.net/05366_sas/*.csv', 'sv=1&sig=2') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_sas';
SELECT JSONExtractString(value, 'source_hash') != '' FROM system.zookeeper WHERE path IN ('/clickhouse/' || currentDatabase() || '/05366_upper', '/clickhouse/' || currentDatabase() || '/05366_sas') AND name = 'metadata' ORDER BY path;
DROP TABLE tq_az_upper;
DROP TABLE tq_az_sas;

-- `send_logs_level` keeps the warnings of the attached tables, and the errors of the refused queries, out of the test output.
SET send_logs_level = 'fatal';

CREATE TABLE tq_a (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_shared/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_shared', processing_threads_num = 1;
CREATE TABLE tq_b (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_shared/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_shared', processing_threads_num = 1, after_processing = 'keep';
-- Keeper has only a hash of the source.
SELECT JSONExtractString(value, 'source_hash') != '', value LIKE '%05366_shared%' FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';

-- Tables with the same `keeper_path` share `after_processing`: after the `ALTER` of `tq_a` both tables have `delete`,
-- while `tq_b` still has `keep` in its definition.
ALTER TABLE tq_a MODIFY SETTING after_processing = 'delete';
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table IN ('tq_a', 'tq_b') AND name = 'after_processing' ORDER BY table;
-- Set `keep` in keeper, as the initiator of a `Replicated` database does before this server runs the query: only the copy of
-- this server still has `delete`, and the `ALTER` to `keep` below must still change it.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '"after_processing":"delete"', '"after_processing":"keep"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
-- `tq_b` already has `keep` in its definition, but an `ALTER` or a `RESET` to `keep` must still change the queue.
-- The same holds for `tq_a`, whose definition already has `delete`.
ALTER TABLE tq_b MODIFY SETTING after_processing = 'keep';
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table IN ('tq_a', 'tq_b') AND name = 'after_processing' ORDER BY table;
ALTER TABLE tq_a MODIFY SETTING after_processing = 'delete';
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table IN ('tq_a', 'tq_b') AND name = 'after_processing' ORDER BY table;
ALTER TABLE tq_b RESET SETTING after_processing;
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table IN ('tq_a', 'tq_b') AND name = 'after_processing' ORDER BY table;
SELECT JSONExtractString(value, 'after_processing') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
-- Leave `keep` in the definition of `tq_a` too, so that both tables can be attached again below.
ALTER TABLE tq_a MODIFY SETTING after_processing = 'keep';
-- The same holds for any setting stored in keeper that an `ALTER` can change, e.g. `processing_threads_num`.
ALTER TABLE tq_a MODIFY SETTING processing_threads_num = 2;
ALTER TABLE tq_b MODIFY SETTING processing_threads_num = 1;
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table IN ('tq_a', 'tq_b') AND name = 'processing_threads_num' ORDER BY table;

-- A setting that the mode does not allow to change can still be repeated with the value of the definition,
-- also when the queue uses the value of another table (`tq_o1` uses 4 threads of `tq_o`).
CREATE TABLE tq_o (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_o/*', NOSIGN, 'CSV') SETTINGS mode = 'ordered', keeper_path = '/clickhouse/{database}/05366_o', processing_threads_num = 4;
CREATE TABLE tq_o1 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_o/*', NOSIGN, 'CSV') SETTINGS mode = 'ordered', keeper_path = '/clickhouse/{database}/05366_o', processing_threads_num = 1;
ALTER TABLE tq_o MODIFY SETTING processing_threads_num = 4;
ALTER TABLE tq_o1 MODIFY SETTING processing_threads_num = 1, polling_max_timeout_ms = 2000;
SELECT create_table_query LIKE '%processing_threads_num = 1%polling_max_timeout_ms = 2000%' FROM system.tables WHERE database = currentDatabase() AND name = 'tq_o1';
ALTER TABLE tq_o MODIFY SETTING processing_threads_num = 5; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tq_o;
DROP TABLE tq_o1;

-- `parallel_inserts` is not stored in keeper, so repeating its value next to another setting changes only the other one.
CREATE TABLE tq_p (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_p/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_p', parallel_inserts = 1;
ALTER TABLE tq_p MODIFY SETTING parallel_inserts = 1, polling_max_timeout_ms = 2000;
SELECT create_table_query LIKE '%parallel_inserts = 1%polling_max_timeout_ms = 2000%' FROM system.tables WHERE database = currentDatabase() AND name = 'tq_p';
DROP TABLE tq_p;

-- `after_processing` is still checked when a table is attached, as on master. A table that fails to attach cannot be
-- detached again in a `Replicated` database, so this uses an `Atomic` one.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s1 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_stale/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_stale', after_processing = 'keep';
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_stale/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_stale', after_processing = 'keep';
-- After the `ALTER` of `tq_s1`, `tq_s2` still has `keep` in its definition and keeper has `delete`.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s1 MODIFY SETTING after_processing = 'delete';
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2; -- { serverError METADATA_MISMATCH }
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s1 MODIFY SETTING after_processing = 'keep';
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2;
SELECT table, value FROM system.s3_queue_settings WHERE database = currentDatabase() || '_1' AND table IN ('tq_s1', 'tq_s2') AND name = 'after_processing' ORDER BY table;

-- Set `after_processing` to `keep` directly in keeper, as another server would, while the copy of this server has `delete`.
-- Then a reattached table and a new table fail instead of taking the outdated `delete`.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s1 MODIFY SETTING after_processing = 'delete';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2 MODIFY SETTING after_processing = 'delete';
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '"after_processing":"delete"', '"after_processing":"keep"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '_1/05366_stale' AND name = 'metadata';
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s2; -- { serverError METADATA_MISMATCH }
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s3 (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_stale/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_stale'; -- { serverError METADATA_MISMATCH }
-- An `ALTER` of another setting needs no `WRITE` grant, so it must not write the outdated `delete` of this server back to keeper.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s1 MODIFY SETTING polling_max_timeout_ms = 2000;
SELECT JSONExtractString(value, 'after_processing') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '_1/05366_stale' AND name = 'metadata';
-- An `ALTER` of `after_processing` itself is written to keeper.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tq_s1 MODIFY SETTING after_processing = 'tag', after_processing_tag_key = 'k', after_processing_tag_value = 'v';
SELECT JSONExtractString(value, 'after_processing') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '_1/05366_stale' AND name = 'metadata';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

-- Set `delete` directly in keeper, as another server would, while the definition and the copy of this server have
-- `keep`. An explicit `keep` must still reach keeper, by a `MODIFY` and by a `RESET`.
CREATE TABLE tq_k (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_k/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_k', after_processing = 'keep';
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '"after_processing":"keep"', '"after_processing":"delete"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_k' AND name = 'metadata';
ALTER TABLE tq_k MODIFY SETTING after_processing = 'keep';
SELECT JSONExtractString(value, 'after_processing'), (SELECT value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table = 'tq_k' AND name = 'after_processing') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_k' AND name = 'metadata';
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '"after_processing":"keep"', '"after_processing":"delete"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_k' AND name = 'metadata';
ALTER TABLE tq_k RESET SETTING after_processing;
SELECT JSONExtractString(value, 'after_processing'), (SELECT value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table = 'tq_k' AND name = 'after_processing') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_k' AND name = 'metadata';
-- An `ALTER` changes only the settings it names: `loading_retries` set by another server stays in keeper.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceOne(value, '"loading_retries":10', '"loading_retries":5') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_k' AND name = 'metadata';
ALTER TABLE tq_k MODIFY SETTING polling_max_timeout_ms = 2000;
SELECT JSONExtractUInt(value, 'loading_retries') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_k' AND name = 'metadata';
DROP TABLE tq_k;

-- Change the source in keeper: the tables must still attach, with a warning.
DETACH TABLE tq_a; DETACH TABLE tq_b;
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceRegexpOne(value, '"source_hash":"[^"]*"', '"source_hash":"00"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
ATTACH TABLE tq_b; ATTACH TABLE tq_a;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('tq_a', 'tq_b');

-- A named collection can change without an `ALTER` of the table, so a table that uses one must still match the source
-- in keeper when it attaches. Changing the source in keeper has the same effect as changing the collection.
CREATE TABLE tq_nc (x String) ENGINE = S3Queue(s3_conn, filename = '05366_nc/*', format = 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_nc';
DETACH TABLE tq_nc;
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceRegexpOne(value, '"source_hash":"[^"]*"', '"source_hash":"00"') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_nc' AND name = 'metadata';
ATTACH TABLE tq_nc; -- { serverError METADATA_MISMATCH }
-- Without a source in keeper, as in metadata of an older version, the table attaches and writes its source there.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceRegexpAll(value, ',"source_hash":"[^"]*"', '') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_nc' AND name = 'metadata';
ATTACH TABLE tq_nc;
DROP TABLE tq_nc;

-- Remove `source` from keeper, as in metadata of an older version: the first attached table writes it back.
-- A new table must not join such metadata, because it could belong to another source.
INSERT INTO system.zookeeper (name, path, value) SELECT name, path, replaceRegexpAll(value, ',"source_hash":"[^"]*"', '') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';
DETACH TABLE tq_a; DETACH TABLE tq_b;
CREATE TABLE tq_c (x String) ENGINE = S3Queue('http://localhost:11111/test/05366_c/*', NOSIGN, 'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05366_shared', processing_threads_num = 1; -- { serverError METADATA_MISMATCH }
ATTACH TABLE tq_b; ATTACH TABLE tq_a;
SELECT JSONExtractString(value, 'source_hash') NOT IN ('', '00') FROM system.zookeeper WHERE path = '/clickhouse/' || currentDatabase() || '/05366_shared' AND name = 'metadata';

DROP TABLE tq_b;
DROP TABLE tq_a;
