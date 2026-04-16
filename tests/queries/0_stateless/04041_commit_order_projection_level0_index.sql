-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- Validate that 0-level parts with `_block_number` in the sorting key
-- have correct values when read, even though `_block_number` may be
-- written incorrectly during insert (corrected at read time).

set enable_analyzer = 1;
set query_plan_optimize_prewhere = 1;
set optimize_move_to_prewhere = 1;
set insert_keeper_fault_injection_probability = 0;
set optimize_use_projections = 1;

drop table if exists mt_level0_idx sync;

CREATE TABLE mt_level0_idx(
    a UInt64,
    projection _commit_order (
        select *, _block_number, _block_offset
        order by _block_number, _block_offset
    )
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1, index_granularity=1;

-- Insert several parts (each is a 0-level part with a single block number)
insert into mt_level0_idx(a) values (30) (10) (20);
insert into mt_level0_idx(a) values (60) (40) (50);
insert into mt_level0_idx(a) values (90) (70) (80);

-- Verify filtering by `_block_number` and `_block_offset` works on 0-level parts
-- (filtering by `_block_number` alone only prunes parts, not granules)
select 'level-0 filter by block_number and block_offset';
explain indexes=1, projections=1 select a from mt_level0_idx where (_block_number, _block_offset) = (2, 1) settings max_threads=1;

-- Verify projection has correct commit order
select '';
select 'projection data';
select a, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'mt_level0_idx', '_commit_order')
settings max_threads=1;

-- Merge and verify the merged part preserves correct values
optimize table mt_level0_idx final;

-- Verify filtering by `_block_number` and `_block_offset` works on 0-level parts
-- (filtering by `_block_number` alone only prunes parts, not granules)
select '';
select 'level-1 filter by block_number and block_offset';
explain indexes=1, projections=1 select a from mt_level0_idx where (_block_number, _block_offset) = (2, 1) settings max_threads=1;

-- Verify projection has correct commit order
select '';
select 'projection data';
select a, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'mt_level0_idx', '_commit_order')
settings max_threads=1;

drop table mt_level0_idx sync;
