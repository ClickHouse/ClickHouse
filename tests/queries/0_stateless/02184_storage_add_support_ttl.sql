-- Tags: log-engine
DROP TABLE IF EXISTS mergeTree_02184;
CREATE TABLE mergeTree_02184 (id UInt64, name String, dt Date) Engine=MergeTree ORDER BY id;
ALTER TABLE mergeTree_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH;
DETACH TABLE mergeTree_02184;
ATTACH TABLE mergeTree_02184;

DROP TABLE IF EXISTS distributed_02184;
CREATE TABLE distributed_02184 (id UInt64, name String, dt Date) Engine=Distributed('test_cluster_two_shards', 'default', 'mergeTree_02184', rand());
ALTER TABLE distributed_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE distributed_02184;
ATTACH TABLE distributed_02184;

DROP TABLE IF EXISTS buffer_02184;
CREATE TABLE buffer_02184 (id UInt64, name String, dt Date) ENGINE = Buffer(default, mergeTree_02184, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
ALTER TABLE buffer_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE buffer_02184;
ATTACH TABLE buffer_02184;

DROP TABLE IF EXISTS merge_02184;
CREATE TABLE merge_02184 (id UInt64, name String, dt Date) ENGINE = Merge('default', 'distributed_02184');
ALTER TABLE merge_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE merge_02184;
ATTACH TABLE merge_02184;

DROP TABLE IF EXISTS null_02184;
CREATE TABLE null_02184 AS system.one Engine=Null();
ALTER TABLE null_02184 MODIFY COLUMN dummy Int TTL now() + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE null_02184;
ATTACH TABLE null_02184;

DROP TABLE IF EXISTS file_02184;
CREATE TABLE file_02184 (id UInt64, name String, dt Date) ENGINE = File(TabSeparated);
ALTER TABLE file_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE file_02184;
ATTACH TABLE file_02184;

DROP TABLE IF EXISTS memory_02184;
CREATE TABLE memory_02184 (id UInt64, name String, dt Date) ENGINE = Memory();
ALTER TABLE memory_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE memory_02184;
ATTACH TABLE memory_02184;

DROP TABLE IF EXISTS log_02184;
CREATE TABLE log_02184 (id UInt64, name String, dt Date) ENGINE = Log();
ALTER TABLE log_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE log_02184;
ATTACH TABLE log_02184;

DROP TABLE IF EXISTS ting_log_02184;
CREATE TABLE ting_log_02184 (id UInt64, name String, dt Date) ENGINE = TinyLog();
ALTER TABLE ting_log_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE ting_log_02184;
ATTACH TABLE ting_log_02184;

DROP TABLE IF EXISTS stripe_log_02184;
CREATE TABLE stripe_log_02184 (id UInt64, name String, dt Date) ENGINE = StripeLog;
ALTER TABLE stripe_log_02184 MODIFY COLUMN name String TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH TABLE stripe_log_02184;
ATTACH TABLE stripe_log_02184;

-- A column-less definition infers its structure from another table. The inferred structure must not
-- carry that table's per-column TTL: these engines do not support TTL, so an ALTER would persist a
-- definition that can no longer be loaded.
DROP TABLE IF EXISTS src_ttl_02184;
CREATE TABLE src_ttl_02184 (id UInt64, name String, dt Date TTL dt + INTERVAL 1 MONTH) Engine=MergeTree ORDER BY id;

DROP TABLE IF EXISTS merge_inferred_02184;
CREATE TABLE merge_inferred_02184 ENGINE = Merge(currentDatabase(), '^src_ttl_02184$');
ALTER TABLE merge_inferred_02184 COMMENT COLUMN id 'x';
DETACH TABLE merge_inferred_02184;
ATTACH TABLE merge_inferred_02184;

DROP TABLE IF EXISTS buffer_inferred_02184;
CREATE TABLE buffer_inferred_02184 ENGINE = Buffer(currentDatabase(), src_ttl_02184, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
ALTER TABLE buffer_inferred_02184 COMMENT COLUMN id 'x';
DETACH TABLE buffer_inferred_02184;
ATTACH TABLE buffer_inferred_02184;

DROP TABLE IF EXISTS distributed_inferred_02184;
CREATE TABLE distributed_inferred_02184 ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'src_ttl_02184', rand());
ALTER TABLE distributed_inferred_02184 COMMENT COLUMN id 'x';
DETACH TABLE distributed_inferred_02184;
ATTACH TABLE distributed_inferred_02184;

DROP TABLE IF EXISTS remote_inferred_02184;
CREATE TABLE remote_inferred_02184 ENGINE = Remote('127.0.0.1', currentDatabase(), src_ttl_02184);
ALTER TABLE remote_inferred_02184 COMMENT COLUMN id 'x';
DETACH TABLE remote_inferred_02184;
ATTACH TABLE remote_inferred_02184;

SELECT name, create_table_query LIKE '%TTL%' FROM system.tables
WHERE database = currentDatabase() AND (name LIKE '%inferred_02184' OR name = 'src_ttl_02184') ORDER BY name;

-- Stripping the inherited TTL must leave the rest of each column alone. A Merge table over sources
-- with differing types infers a supertype, and a codec that is only valid for the source type rides
-- along on it, so re-validating a column here would reject a structure that reads fine.
SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS qv_narrow_02184;
DROP TABLE IF EXISTS qv_wide_02184;
CREATE TABLE qv_narrow_02184 (id UInt32, dt Date, vec Array(Float32) CODEC(Quantized('rabitq', 64)) TTL dt + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY id;
CREATE TABLE qv_wide_02184 (id UInt32, dt Date, vec String) ENGINE = MergeTree ORDER BY id;

SELECT count() FROM merge(currentDatabase(), '^qv_(narrow|wide)_02184$');

DROP TABLE IF EXISTS qv_merge_02184;
CREATE TABLE qv_merge_02184 ENGINE = Merge(currentDatabase(), '^qv_(narrow|wide)_02184$');
SELECT count() FROM qv_merge_02184;
-- Guards the case above against going vacuous: the codec is only in conflict while `vec` is inferred
-- as a supertype that is not an Array of floats.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'qv_merge_02184' AND name = 'vec';

-- The other three engines take their structure from a single table, so the same conflict reaches them
-- through the Merge table above.
DROP TABLE IF EXISTS qv_buffer_02184;
CREATE TABLE qv_buffer_02184 ENGINE = Buffer(currentDatabase(), qv_merge_02184, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

DROP TABLE IF EXISTS qv_distributed_02184;
CREATE TABLE qv_distributed_02184 ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'qv_merge_02184', rand());

DROP TABLE IF EXISTS qv_remote_02184;
CREATE TABLE qv_remote_02184 ENGINE = Remote('127.0.0.1', currentDatabase(), qv_merge_02184);

-- src_ttl_02184's row above is the negative control for this one: qv_narrow_02184 keeps its own TTL,
-- so a strip that reached too far would show up here as a 0.
SELECT name, create_table_query LIKE '%TTL%' FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'qv_%_02184' ORDER BY name;

-- Copying that structure to another TTL-less engine writes an explicit column list, so the conflict
-- has to be refused here and not persisted into a table that would fail to load afterwards.
DROP TABLE IF EXISTS qv_copy_02184;
CREATE TABLE qv_copy_02184 ENGINE = Memory AS qv_merge_02184; -- { serverError ILLEGAL_COLUMN }

-- Clearing the TTL must not rebuild the column either: an ALIAS column has no real subcolumns, so
-- registering them makes a subcolumn read of the alias fail.
DROP TABLE IF EXISTS qv_alias_02184;
CREATE TABLE qv_alias_02184 (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr TTL dt + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY id;
INSERT INTO qv_alias_02184 VALUES (1, '2026-01-01', [1, 2, 3]);

DROP TABLE IF EXISTS qv_alias_merge_02184;
CREATE TABLE qv_alias_merge_02184 ENGINE = Merge(currentDatabase(), '^qv_alias_02184$');
SELECT al, al.size0 FROM qv_alias_merge_02184;

DROP TABLE IF EXISTS qv_alias_buffer_02184;
CREATE TABLE qv_alias_buffer_02184 ENGINE = Buffer(currentDatabase(), qv_alias_02184, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT al, al.size0 FROM qv_alias_buffer_02184;
