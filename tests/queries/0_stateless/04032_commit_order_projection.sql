-- Tags: no-parallel-replicas

set enable_analyzer = 1;

drop table if exists mt_with_commit_order sync;

CREATE TABLE mt_with_commit_order(
    a UInt64,
    projection _commit_order (
        select *, _block_number, _block_offset
        order by _block_number, _block_offset
    )
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1, index_granularity=1;

insert into mt_with_commit_order(a) values (8) (5) (2);   -- all_1_1_0
insert into mt_with_commit_order(a) values (10) (7) (9);  -- all_2_2_0
insert into mt_with_commit_order(a) values (6) (3) (4);   -- all_3_3_0
insert into mt_with_commit_order(a) values (12) (1) (11); -- all_4_4_0
optimize table mt_with_commit_order final;                -- all_1_4_1

select 'sorted by a';
select a, _block_number, _block_offset, _part from mt_with_commit_order settings max_threads=1;

select '';
select 'sorted by (block number, block offset)';
select a, _block_number, _block_offset, _part from mergeTreeProjection(currentDatabase(), 'mt_with_commit_order', '_commit_order') settings max_threads=1;

select '';
select 'check block number / offset mapping';
select a, lhs._block_number = rhs._block_number as block_number_match, lhs._block_offset = rhs._block_offset as block_offset_match from mt_with_commit_order as lhs join mergeTreeProjection(currentDatabase(), 'mt_with_commit_order', '_commit_order') as rhs using a order by a;

select '';
select 'index lookup';
SELECT explain FROM (explain indexes=1, projections=1 select a, _block_number, _block_offset from mt_with_commit_order where (_block_number, _block_offset) = (3, 1) settings optimize_use_projections=1) WHERE explain NOT LIKE '%Condition%';

drop table mt_with_commit_order sync;
