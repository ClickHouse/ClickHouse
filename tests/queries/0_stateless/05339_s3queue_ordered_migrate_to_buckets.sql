-- Tags: no-fasttest, zookeeper, no-shared-catalog
-- no-fasttest: the `S3Queue` table engine is not built in the fast test.
-- zookeeper: the migration moves the `processed` nodes of the table inside Keeper.
-- no-shared-catalog: with a shared catalog each `ALTER TABLE ... MODIFY SETTING` below becomes
-- a distributed DDL over the hosts of the test cluster and can outlast
-- `distributed_ddl_task_timeout` under load; this test targets the Keeper layout of the queue
-- metadata, not the catalog.

-- An Ordered queue without buckets keeps its last processed file name in `<keeper_path>/processed`;
-- with buckets, in one `buckets/<bucket>/processed` node each. `ALTER TABLE ... MODIFY SETTING
-- buckets = N` moves the old node into the new layout. It is accepted only together with
-- `s3queue_migrate_old_metadata_to_buckets = 1` and only while `buckets` is still unset.
--
-- Nothing here reads object storage: no materialized view is attached, so only Keeper is
-- touched. The engine setting `last_processed_path` is what makes CREATE TABLE write the
-- `processed` node, so the old layout exists without any file being processed.
-- `processing_threads_num = 1` is pinned everywhere, because an unset `buckets` falls back to
-- it and the migration only runs while the queue still has a single bucket.

-- Keep this file's DDL out of the AST fuzzer: a replayed `CREATE` is a second queue on the same `keeper_path`,
-- whose definition keeps no `buckets` after the migration below, so the next server start rejects it.
SET ast_fuzzer_any_query = 0;

-- 1. The old node exists: it is removed and recreated under both new buckets.

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

-- 2. The migration removes the old node and creates the bucket nodes in one Keeper
--    transaction, so when the removal is rejected the whole migration must be rejected and
--    the old pointer must survive. The removal is made to fail deterministically by giving
--    the old node a child: the replicated table below keeps its metadata under it. This is
--    the generic "the transaction did not go through" branch and nothing more: the code
--    reports every non-OK response to the removal as `SUPPORT_IS_DISABLED`, whatever the
--    cause, so this case makes no claim about concurrent migrations by other replicas.
--    The holder is deliberately not a second queue: a queue removes its whole `keeper_path`
--    when its last registration goes, so a holder queue would be left without its own
--    registry node whenever `q_block` happened to be dropped first, which aborts a debug
--    build. A replicated table only turns readonly when its path disappears.

CREATE TABLE q_block (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05339_block/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, last_processed_path = 'block.csv',
         keeper_path = '/clickhouse/{database}/q_block';

CREATE TABLE q_block_holder (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/q_block/processed/holder', '{replica}')
ORDER BY tuple();

SELECT 'block / children before', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_block/processed';

ALTER TABLE q_block MODIFY SETTING buckets = 2; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'block / buckets setting', count() FROM system.tables
WHERE database = currentDatabase() AND name = 'q_block' AND create_table_query LIKE '%buckets =%';
SELECT 'block / pointer after', count(), max(JSONExtractString(value, 'file_path'))
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_block' AND name = 'processed';
SELECT 'block / bucket 0 after', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_block/buckets/0' AND name = 'processed';

-- The holder has to go before `q_block`, so that the replicated table removes its own Keeper
-- path while the queue path it lives under still exists.
DROP TABLE q_block_holder;
DROP TABLE q_block;
DROP TABLE q_unset;
