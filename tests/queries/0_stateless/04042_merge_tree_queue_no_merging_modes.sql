-- Tags: no-random-merge-tree-settings
-- Verify that `MergeTreeQueue` does not support merging modes, ORDER BY, PRIMARY KEY,
-- PARTITION BY, the deprecated engine syntax, or disabling the virtual columns of its
-- sorting key, and that it is gated behind an experimental setting.

select 'the engines are experimental';
CREATE TABLE mtq_gated(a UInt64) ENGINE = MergeTreeQueue; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE mtq_gated(a UInt64) ENGINE = ReplicatedMergeTreeQueue('/clickhouse/tables/{database}/mtq_gated', '1'); -- { serverError SUPPORT_IS_DISABLED }
-- An `ATTACH` that carries a full table definition is CREATE-like user input, so it must not
-- bypass the gate; only replayed metadata (short `ATTACH`, server startup) is exempt.
ATTACH TABLE mtq_gated UUID 'aa4e2eb5-8b96-4d1a-9c5c-5e0a4a34cb01' (a UInt64) ENGINE = MergeTreeQueue; -- { serverError SUPPORT_IS_DISABLED }

set allow_experimental_merge_tree_queue = 1;

select 'SummingMergeTreeQueue';
CREATE TABLE mtq_summing(a UInt64) ENGINE = SummingMergeTreeQueue ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'ReplacingMergeTreeQueue';
CREATE TABLE mtq_replacing(a UInt64) ENGINE = ReplacingMergeTreeQueue ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'CollapsingMergeTreeQueue';
CREATE TABLE mtq_collapsing(a UInt64, s Int8) ENGINE = CollapsingMergeTreeQueue(s) ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'AggregatingMergeTreeQueue';
CREATE TABLE mtq_aggregating(a UInt64) ENGINE = AggregatingMergeTreeQueue ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'ORDER BY is forbidden';
CREATE TABLE mtq_order(a UInt64) ENGINE = MergeTreeQueue ORDER BY a; -- { serverError BAD_ARGUMENTS }
-- An explicit sort direction never matches the plain ascending commit-order key and must be
-- rejected the same way, even when the column names match (used to throw a logical error).
CREATE TABLE mtq_order(a UInt64) ENGINE = MergeTreeQueue ORDER BY (_block_number DESC, _block_offset DESC); -- { serverError BAD_ARGUMENTS }
CREATE TABLE mtq_order(a UInt64) ENGINE = MergeTreeQueue ORDER BY a DESC; -- { serverError BAD_ARGUMENTS }

select 'PRIMARY KEY is forbidden';
CREATE TABLE mtq_pk(a UInt64) ENGINE = MergeTreeQueue PRIMARY KEY a; -- { serverError BAD_ARGUMENTS }

select 'PARTITION BY is forbidden';
CREATE TABLE mtq_partitioned(p UInt64, a UInt64) ENGINE = MergeTreeQueue PARTITION BY p; -- { serverError BAD_ARGUMENTS }
-- A full-definition `ATTACH` must not bypass the check either.
ATTACH TABLE mtq_partitioned UUID 'aa4e2eb5-8b96-4d1a-9c5c-5e0a4a34cb02' (p UInt64, a UInt64) ENGINE = MergeTreeQueue PARTITION BY p; -- { serverError BAD_ARGUMENTS }

select 'ALTER MODIFY ORDER BY is forbidden';
drop table if exists mtq_alter_order sync;
CREATE TABLE mtq_alter_order(a UInt64) ENGINE = MergeTreeQueue;
ALTER TABLE mtq_alter_order MODIFY ORDER BY a; -- { serverError BAD_ARGUMENTS }

select 'disabling the block number and block offset columns is forbidden';
ALTER TABLE mtq_alter_order MODIFY SETTING enable_block_number_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mtq_alter_order MODIFY SETTING enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mtq_alter_order RESET SETTING enable_block_number_column; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mtq_alter_order RESET SETTING enable_block_offset_column; -- { serverError BAD_ARGUMENTS }
-- Enabling them again is a no-op and must be allowed.
ALTER TABLE mtq_alter_order MODIFY SETTING enable_block_number_column = 1;
drop table mtq_alter_order sync;

select 'no explicit key is required';
set create_table_empty_primary_key_by_default = 0;
drop table if exists mtq_no_key sync;
CREATE TABLE mtq_no_key(a UInt64) ENGINE = MergeTreeQueue;
select sorting_key from system.tables where database = currentDatabase() and name = 'mtq_no_key';
drop table mtq_no_key sync;

select 'existing tables are attached without the experimental setting';
drop table if exists mtq_reattach sync;
CREATE TABLE mtq_reattach(a UInt64) ENGINE = MergeTreeQueue;
DETACH TABLE mtq_reattach;
set allow_experimental_merge_tree_queue = 0;
ATTACH TABLE mtq_reattach;
select count() from mtq_reattach;
drop table mtq_reattach sync;
set allow_experimental_merge_tree_queue = 1;

select 'deprecated syntax is forbidden';
set allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE mtq_deprecated(d Date, a UInt64) ENGINE = MergeTreeQueue(d, a, 8192); -- { serverError BAD_ARGUMENTS }
