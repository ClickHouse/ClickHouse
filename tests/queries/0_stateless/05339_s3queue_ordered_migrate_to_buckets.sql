-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` table engine is not built in the fast test.
-- zookeeper: the migration moves the `processed` nodes of the table inside Keeper.

-- An Ordered queue without buckets keeps its last processed file name in one
-- `<keeper_path>/processed` node; with buckets, in one `buckets/<bucket>/processed` node each.
-- `ALTER TABLE ... MODIFY SETTING buckets = N` moves the old node into the new layout. It is
-- accepted only together with `s3queue_migrate_old_metadata_to_buckets = 1` and only while
-- `buckets` is still 0 or 1.
--
-- Nothing here reads object storage: no materialized view is attached, so only Keeper is
-- touched. The engine setting `last_processed_path` is what makes CREATE TABLE write the
-- `processed` node, so the old layout exists without any file being processed.
-- `processing_threads_num = 1` is pinned everywhere, because an unset `buckets` falls back to
-- it.

-- 1. `buckets` was never set: the old node exists and is moved into both new buckets.

CREATE TABLE q_unset (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05339_unset/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, last_processed_path = 'unset.csv',
         keeper_path = '/clickhouse/{database}/q_unset';

SELECT 'unset / processed before', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_unset' AND name = 'processed';
SELECT 'unset / buckets before', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_unset' AND name = 'buckets';

-- Without the user setting the migration is refused and the old layout stays untouched.
ALTER TABLE q_unset MODIFY SETTING buckets = 2; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'unset / processed after refusal', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_unset' AND name = 'processed';

SET s3queue_migrate_old_metadata_to_buckets = 1;
ALTER TABLE q_unset MODIFY SETTING buckets = 2;

SELECT 'unset / buckets setting', extract(create_table_query, 'buckets = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 'q_unset';
SELECT 'unset / processed after', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_unset' AND name = 'processed';
SELECT 'unset / bucket 0', count(), max(JSONExtractString(value, 'file_path'))
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_unset/buckets/0' AND name = 'processed';
SELECT 'unset / bucket 1', count(), max(JSONExtractString(value, 'file_path'))
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_unset/buckets/1' AND name = 'processed';

-- The new layout is in place now, so a second migration is refused.
ALTER TABLE q_unset MODIFY SETTING buckets = 4; -- { serverError SUPPORT_IS_DISABLED }

-- 2. `buckets = 1` is the old layout too, but the node the migration reads lives under
--    bucket 0 and does not exist, so there is nothing to move.

CREATE TABLE q_one (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05339_one/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, buckets = 1,
         last_processed_path = 'one.csv',
         keeper_path = '/clickhouse/{database}/q_one';

ALTER TABLE q_one MODIFY SETTING buckets = 2;

SELECT 'one / buckets setting', extract(create_table_query, 'buckets = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 'q_one';
SELECT 'one / processed after', count(), max(JSONExtractString(value, 'file_path'))
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_one' AND name = 'processed';
SELECT 'one / buckets after', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_one' AND name = 'buckets';

-- 3. Another replica migrated the same table a moment earlier, so the bucket node that the
--    migration wants to create for bucket 1 is already there. The two helper queues below
--    leave their own `processed` node exactly where that replica would have left it: their
--    `keeper_path` is a bucket path of the migrated queue. The migration must notice the
--    conflict and return without touching what the other replica wrote.

CREATE TABLE q_conc (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05339_conc/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, buckets = 1,
         last_processed_path = 'conc.csv',
         keeper_path = '/clickhouse/{database}/q_conc';

CREATE TABLE q_conc_b0 (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05339_conc_b0/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, last_processed_path = 'bucket0.csv',
         keeper_path = '/clickhouse/{database}/q_conc/buckets/0';

CREATE TABLE q_conc_b1 (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05339_conc_b1/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, last_processed_path = 'bucket1.csv',
         keeper_path = '/clickhouse/{database}/q_conc/buckets/1';

SELECT 'conc / bucket 1 before', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_conc/buckets/1' AND name = 'processed';

ALTER TABLE q_conc MODIFY SETTING buckets = 2;

SELECT 'conc / buckets setting', extract(create_table_query, 'buckets = (\\d+)')
FROM system.tables WHERE database = currentDatabase() AND name = 'q_conc';
SELECT 'conc / bucket 0', count(), max(JSONExtractString(value, 'file_path'))
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_conc/buckets/0' AND name = 'processed';
SELECT 'conc / bucket 1', count(), max(JSONExtractString(value, 'file_path'))
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_conc/buckets/1' AND name = 'processed';

-- The five tables are left to the teardown of the test database, which drops them in one
-- batch. An explicit `DROP TABLE` costs about a second each here: the test profile waits for
-- the dropped table to leave the drop queue of DatabaseCatalog, and that queue is polled once
-- a second (tests/config/users.d/database_atomic_drop_detach_sync.xml).
