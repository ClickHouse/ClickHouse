-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- Basic test for `MergeTreeQueue` engine: validates that rows preserve
-- insertion (commit) order via `_block_number` and `_block_offset`
-- virtual columns automatically appended to the sorting key.

set enable_analyzer = 1;
set query_plan_optimize_prewhere = 1;
set optimize_move_to_prewhere = 1;
set insert_keeper_fault_injection_probability = 0;

drop table if exists mtq_basic sync;

CREATE TABLE mtq_basic(a UInt64)
ENGINE = MergeTreeQueue
settings index_granularity=1;

-- Insert several parts
insert into mtq_basic values (30) (10) (20);

detach table mtq_basic;
attach table mtq_basic;

insert into mtq_basic values (60) (40) (50);

detach table mtq_basic;
attach table mtq_basic;

insert into mtq_basic values (90) (70) (80);

-- Level-0 parts: data is sorted by commit order without explicit ORDER BY
select 'level-0 data';
select a, _block_number, _block_offset from mtq_basic settings max_threads=1;

-- Verify primary index has correct `_block_number` values via `mergeTreeIndex`
select '';
select 'level-0 primary index';
select part_name, _block_number, _block_offset
from mergeTreeIndex(currentDatabase(), 'mtq_basic')
order by part_name, mark_number;

-- Verify sorting key includes virtual columns
select '';
select 'sorting key';
select sorting_key from system.tables where database = currentDatabase() and name = 'mtq_basic';

-- Index lookup on level-0 parts
select '';
select 'level-0 index lookup';
select a from mtq_basic where (_block_number, _block_offset) = (2, 1);

select '';
select 'level-0 index lookup explain';
explain indexes=1 select a from mtq_basic where (_block_number, _block_offset) = (2, 1);

-- Merge and verify commit order is preserved
optimize table mtq_basic final;

select '';
select 'after merge';
select a, _block_number, _block_offset from mtq_basic settings max_threads=1;

-- Index lookup after merge
select '';
select 'after merge index lookup';
select a from mtq_basic where (_block_number, _block_offset) = (2, 1);

select '';
select 'after merge index lookup explain';
explain indexes=1 select a from mtq_basic where (_block_number, _block_offset) = (2, 1);

drop table mtq_basic sync;
