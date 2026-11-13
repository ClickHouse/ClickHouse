-- Tags: no-parallel-replicas

set enable_analyzer = 1;
set mutations_sync = 2;

drop table if exists mt_materialize sync;

CREATE TABLE mt_materialize(
    a UInt64,
    b UInt64,
    projection _commit_order (
        select *, _block_number, _block_offset
        order by _block_number, _block_offset
    )
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1;

insert into mt_materialize values (3, 30) (1, 10) (2, 20);
insert into mt_materialize values (6, 60) (4, 40) (5, 50);

-- Level-0 parts: no projection
select 'before materialize';
select count() from system.projection_parts
    where database = currentDatabase() and table = 'mt_materialize' and active;

-- MATERIALIZE PROJECTION should materialize projections
alter table mt_materialize materialize projection _commit_order;

select 'after materialize';
select count() from system.projection_parts
    where database = currentDatabase() and table = 'mt_materialize' and active;

-- Verify projection data and payload columns
select 'projection data';
select a, b, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'mt_materialize', '_commit_order')
where _part = 'all_2_2_0_3'
settings max_threads=1;

-- Mutation on merged part (level > 0) should correctly rebuild the projection
alter table mt_materialize update b = b + 1 where a = 1;

select 'after mutation on merged part';
select a, b, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'mt_materialize', '_commit_order')
where _part = 'all_1_1_0_4'
settings max_threads=1;

drop table mt_materialize sync;
