-- Tags: no-parallel-replicas

set enable_analyzer = 1;
set insert_keeper_fault_injection_probability = 0;

drop table if exists rmt_commit_order sync;

CREATE TABLE rmt_commit_order(
    a UInt64,
    projection _commit_order (
        select *, _block_number, _block_offset
        order by _block_number, _block_offset
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt_commit_order', '1')
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1;

insert into rmt_commit_order(a) values (8) (5) (2);
insert into rmt_commit_order(a) values (10) (7) (9);
insert into rmt_commit_order(a) values (6) (3) (4);

-- Level-0 parts should NOT have the projection
select 'before merge: projection parts';
select count() from system.projection_parts
    where database = currentDatabase() and table = 'rmt_commit_order' and active;

optimize table rmt_commit_order final;

-- After merge, projection should exist with correct data
select 'after merge: projection parts';
select count() from system.projection_parts
    where database = currentDatabase() and table = 'rmt_commit_order' and active;

select 'projection data';
select a, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'rmt_commit_order', '_commit_order')
settings max_threads=1;

drop table rmt_commit_order sync;
