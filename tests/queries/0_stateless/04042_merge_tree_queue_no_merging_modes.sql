-- Tags: no-random-merge-tree-settings
-- Verify that `MergeTreeQueue` does not support merging modes, ORDER BY, or PRIMARY KEY.

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

select 'PRIMARY KEY is forbidden';
CREATE TABLE mtq_pk(a UInt64) ENGINE = MergeTreeQueue PRIMARY KEY a; -- { serverError BAD_ARGUMENTS }

select 'ALTER MODIFY ORDER BY is forbidden';
drop table if exists mtq_alter_order sync;
CREATE TABLE mtq_alter_order(a UInt64) ENGINE = MergeTreeQueue;
ALTER TABLE mtq_alter_order MODIFY ORDER BY a; -- { serverError BAD_ARGUMENTS }
drop table mtq_alter_order sync;
