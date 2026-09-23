-- Tags: zookeeper

-- Async inserts routed through a StorageAlias must deduplicate on the resolved target's unified hash.
-- The async insert queue keys the batch by the Alias table, but the flushed chunk is written by the
-- target's ReplicatedMergeTreeSink, which performs the unified-hash deduplication governed by the
-- target's replicated_deduplication_window. Two identical async inserts must collapse to a single copy.

SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS alias_async_dedup_table;
DROP TABLE IF EXISTS alias_async_dedup_target SYNC;

CREATE TABLE alias_async_dedup_target (id UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/alias_async_dedup_target', '1')
ORDER BY id;

CREATE TABLE alias_async_dedup_table ENGINE = Alias('alias_async_dedup_target');

INSERT INTO alias_async_dedup_table SETTINGS async_insert = 1, wait_for_async_insert = 1, deduplicate_insert = 'enable' VALUES (1), (2), (3);
INSERT INTO alias_async_dedup_table SETTINGS async_insert = 1, wait_for_async_insert = 1, deduplicate_insert = 'enable' VALUES (1), (2), (3);

SELECT 'alias_async_dedup_enabled', count() FROM alias_async_dedup_target;

-- Negative control: with the target's unified window disabled, the same async inserts through the
-- Alias are not deduplicated and both batches land. This confirms the deduplication above is real and
-- that the Alias honors the resolved target's deduplication settings.
DROP TABLE IF EXISTS alias_async_nodedup_table;
DROP TABLE IF EXISTS alias_async_nodedup_target SYNC;

CREATE TABLE alias_async_nodedup_target (id UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/alias_async_nodedup_target', '1')
ORDER BY id
SETTINGS replicated_deduplication_window = 0;

CREATE TABLE alias_async_nodedup_table ENGINE = Alias('alias_async_nodedup_target');

INSERT INTO alias_async_nodedup_table SETTINGS async_insert = 1, wait_for_async_insert = 1, deduplicate_insert = 'enable' VALUES (1), (2), (3);
INSERT INTO alias_async_nodedup_table SETTINGS async_insert = 1, wait_for_async_insert = 1, deduplicate_insert = 'enable' VALUES (1), (2), (3);

SELECT 'alias_async_dedup_disabled', count() FROM alias_async_nodedup_target;

DROP TABLE alias_async_dedup_table;
DROP TABLE alias_async_dedup_target SYNC;
DROP TABLE alias_async_nodedup_table;
DROP TABLE alias_async_nodedup_target SYNC;
