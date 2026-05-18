-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- Tests for ALTER TABLE and mutations on MergeTreeQueue engine.

set enable_analyzer = 1;
set query_plan_optimize_prewhere = 1;
set optimize_move_to_prewhere = 1;
set insert_keeper_fault_injection_probability = 0;
set alter_sync = 2;
set mutations_sync = 2;

-- ============================================================
-- 1. Empty table
-- ============================================================
select '== 1. Empty table ==';
drop table if exists mtq_empty sync;
CREATE TABLE mtq_empty(a UInt64) ENGINE = MergeTreeQueue settings index_granularity=1;

select 'empty table';
select count() from mtq_empty;

select '';
select 'empty table sorting key';
select sorting_key from system.tables where database = currentDatabase() and name = 'mtq_empty';

DETACH TABLE mtq_empty;
ATTACH TABLE mtq_empty;

select '';
select 'empty table sorting key';
select sorting_key from system.tables where database = currentDatabase() and name = 'mtq_empty';

drop table mtq_empty sync;

-- ============================================================
-- 2. ALTER ADD/DROP COLUMN
-- ============================================================
select '';
select '== 2. ALTER ADD/DROP COLUMN ==';
drop table if exists mtq_alter sync;
CREATE TABLE mtq_alter(a UInt64) ENGINE = MergeTreeQueue settings index_granularity=1;

insert into mtq_alter values (10) (20) (30);

select 'before alter add column';
select a, _block_number, _block_offset from mtq_alter settings max_threads=1;

ALTER TABLE mtq_alter ADD COLUMN b String DEFAULT 'x';

insert into mtq_alter values (40, 'y') (50, 'z');

select '';
select 'after alter add column';
select a, b, _block_number, _block_offset from mtq_alter settings max_threads=1;

select '';
select 'sorting key after alter';
select sorting_key from system.tables where database = currentDatabase() and name = 'mtq_alter';

-- Index lookup still works after schema change
select '';
select 'index lookup after alter';
select a, b from mtq_alter where (_block_number, _block_offset) = (2, 0);

select '';
select 'index lookup after alter explain';
explain indexes=1 select a, b from mtq_alter where (_block_number, _block_offset) = (2, 0);

ALTER TABLE mtq_alter DROP COLUMN b;

select '';
select 'after drop column';
select a, _block_number, _block_offset from mtq_alter settings max_threads=1;

optimize table mtq_alter final;

select '';
select 'after alter merge';
select a, _block_number, _block_offset from mtq_alter settings max_threads=1;

ALTER TABLE mtq_alter ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE mtq_alter DROP COLUMN c;

select '';
select 'after drop column on merged';
select a, _block_number, _block_offset from mtq_alter settings max_threads=1;

-- Detach/attach to verify metadata persistence
DETACH TABLE mtq_alter;
ATTACH TABLE mtq_alter;

select '';
select 'after detach/attach';
select a, _block_number, _block_offset from mtq_alter settings max_threads=1;

select '';
select 'sorting key after detach/attach';
select sorting_key from system.tables where database = currentDatabase() and name = 'mtq_alter';

drop table mtq_alter sync;

-- ============================================================
-- 3. DELETE mutation
-- ============================================================
select '';
select '== 3. DELETE mutation ==';
drop table if exists mtq_delete sync;
CREATE TABLE mtq_delete(a UInt64) ENGINE = MergeTreeQueue settings index_granularity=1;

insert into mtq_delete values (10) (20) (30);
insert into mtq_delete values (40) (50) (60);

-- Delete on 0-level parts
ALTER TABLE mtq_delete DELETE WHERE a = 20 SETTINGS mutations_sync = 1;

select 'after delete';
select a, _block_number, _block_offset from mtq_delete settings max_threads=1;

select '';
select 'after delete index lookup';
select a from mtq_delete where (_block_number, _block_offset) = (2, 1);

select '';
select 'after delete index explain';
explain indexes=1 select a from mtq_delete where (_block_number, _block_offset) = (2, 1);

-- Detach/attach then delete on reattached table
DETACH TABLE mtq_delete;
ATTACH TABLE mtq_delete;

optimize table mtq_delete final;

ALTER TABLE mtq_delete DELETE WHERE a = 40 SETTINGS mutations_sync = 1;

select '';
select 'after detach/attach then merge then delete';
select a, _block_number, _block_offset from mtq_delete settings max_threads=1;

drop table mtq_delete sync;

-- ============================================================
-- 4. UPDATE mutation
-- ============================================================
select '';
select '== 4. UPDATE mutation ==';
drop table if exists mtq_update sync;
CREATE TABLE mtq_update(a UInt64) ENGINE = MergeTreeQueue settings index_granularity=1;

insert into mtq_update values (10) (20) (30);
insert into mtq_update values (40) (50) (60);

-- Update on 0-level parts
ALTER TABLE mtq_update UPDATE a = a + 100 WHERE a >= 30 SETTINGS mutations_sync = 1;

select 'after update';
select a, _block_number, _block_offset from mtq_update settings max_threads=1;

select '';
select 'after update index lookup';
select a from mtq_update where (_block_number, _block_offset) = (2, 1);

select '';
select 'after update index explain';
explain indexes=1 select a from mtq_update where (_block_number, _block_offset) = (2, 1);

-- Update by commit-order position on 0-level parts
ALTER TABLE mtq_update UPDATE a = 999 WHERE (_block_number, _block_offset) = (1, 0) SETTINGS mutations_sync = 1;
ALTER TABLE mtq_update UPDATE a = 555 WHERE (_block_number, _block_offset) = (1, 1) SETTINGS mutations_sync = 1;

select '';
select 'after update by index';
select a, _block_number, _block_offset from mtq_update settings max_threads=1;

-- Merge then update again on merged part
optimize table mtq_update final;

ALTER TABLE mtq_update UPDATE a = a + 1000 WHERE a >= 500 SETTINGS mutations_sync = 1;

select '';
select 'after merge then update';
select a, _block_number, _block_offset from mtq_update settings max_threads=1;

-- Update by index on merged part
ALTER TABLE mtq_update UPDATE a = 7777 WHERE (_block_number, _block_offset) = (2, 0) SETTINGS mutations_sync = 1;

select '';
select 'after merge then update by index';
select a, _block_number, _block_offset from mtq_update settings max_threads=1;

-- Detach/attach then update on reattached table
DETACH TABLE mtq_update;
ATTACH TABLE mtq_update;

ALTER TABLE mtq_update UPDATE a = a + 10000 WHERE (_block_number, _block_offset) = (1, 2) SETTINGS mutations_sync = 1;

select '';
select 'after detach/attach then update';
select a, _block_number, _block_offset from mtq_update settings max_threads=1;

drop table mtq_update sync;
