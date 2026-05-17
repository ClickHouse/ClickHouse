-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree, no-parallel-replicas

set enable_analyzer = 1;

drop table if exists mt_merge_levels sync;

CREATE TABLE mt_merge_levels(
    a UInt64,
    projection _commit_order (
        select *, _block_number, _block_offset
        order by _block_number, _block_offset
    )
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1, merge_selector_algorithm = 'Manual';

-- Create 2 level-1 parts by inserting pairs and merging
insert into mt_merge_levels(a) values (4) (1);
insert into mt_merge_levels(a) values (3) (6);
insert into mt_merge_levels(a) values (8) (5);
insert into mt_merge_levels(a) values (2) (7);

SYSTEM SCHEDULE MERGE mt_merge_levels PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SCHEDULE MERGE mt_merge_levels PARTS 'all_3_3_0', 'all_4_4_0';
SYSTEM SYNC MERGES mt_merge_levels;

-- Now merge level-1 + level-1 -> level-2 (projection MERGED, not rebuilt)
SYSTEM SCHEDULE MERGE mt_merge_levels PARTS 'all_1_2_1', 'all_3_4_1';
SYSTEM SYNC MERGES mt_merge_levels;

-- Check part_log ProfileEvents to verify rebuild vs merge behavior
system flush logs part_log;

select 'part_log: rebuild merges';
select part_name, ProfileEvents['RebuiltProjections'] as rebuilt, ProfileEvents['MergedProjections'] as merged
from system.part_log
where database = currentDatabase() and table = 'mt_merge_levels' and event_type = 'MergeParts' and part_name in ('all_1_2_1', 'all_3_4_1')
order by part_name;

select 'part_log: projection merges';
select part_name, ProfileEvents['RebuiltProjections'] as rebuilt, ProfileEvents['MergedProjections'] as merged
from system.part_log
where database = currentDatabase() and table = 'mt_merge_levels' and event_type = 'MergeParts' and part_name = 'all_1_4_2'
order by part_name;

drop table mt_merge_levels sync;
