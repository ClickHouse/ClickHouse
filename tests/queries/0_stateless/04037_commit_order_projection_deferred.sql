-- Tags: no-parallel-replicas

set enable_analyzer = 1;

drop table if exists mt_deferred sync;

CREATE TABLE mt_deferred(
    a UInt64,
    projection _commit_order (
        select *, _block_number, _block_offset
        order by _block_number, _block_offset
    )
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1;

-- Insert several parts
insert into mt_deferred(a) values (3) (1) (2);
insert into mt_deferred(a) values (6) (4) (5);

-- Level-0 parts should NOT have the projection
select 'before merge: projection parts count';
select count() from system.projection_parts
    where database = currentDatabase() and table = 'mt_deferred' and active;

-- Force merge
optimize table mt_deferred final;

-- After merge, the projection should be materialized with correct data
select 'after merge: projection parts count';
select count() from system.projection_parts
where database = currentDatabase() and table = 'mt_deferred' and active;

-- Verify projection data has correct commit order
select 'projection data';
select a, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'mt_deferred', '_commit_order')
settings max_threads=1;

drop table mt_deferred sync;
