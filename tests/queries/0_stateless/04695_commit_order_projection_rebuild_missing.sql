-- Tags: no-parallel-replicas
-- A commit-order projection is missing from parts written before it existed (the upgrade case:
-- such parts were produced when commit-order projections were not materialized on insert).
-- A merge must rebuild the projection for the merged part instead of silently dropping it,
-- even with the default `materialize_projections_on_merge = 0`.

set enable_analyzer = 1;

drop table if exists mt_rebuild sync;

CREATE TABLE mt_rebuild(a UInt64)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column = 1, enable_block_offset_column = 1,
         allow_commit_order_projection = 1, materialize_projections_on_merge = 0;

-- These parts are written without the projection.
insert into mt_rebuild values (3) (1) (2);
insert into mt_rebuild values (6) (4) (5);

ALTER TABLE mt_rebuild ADD PROJECTION _commit_order (
    select *, _block_number, _block_offset
    order by _block_number, _block_offset
);

select 'parts without the projection';
select count() from system.parts where database = currentDatabase() and table = 'mt_rebuild' and active;
select count() from system.projection_parts
    where database = currentDatabase() and table = 'mt_rebuild' and active and name = '_commit_order';

set optimize_throw_if_noop = 1;
optimize table mt_rebuild final;

select 'the merged part has the projection rebuilt';
select count() from system.parts where database = currentDatabase() and table = 'mt_rebuild' and active;
select count() from system.projection_parts
    where database = currentDatabase() and table = 'mt_rebuild' and active and name = '_commit_order';

select 'projection data';
select a, _block_number, _block_offset
from mergeTreeProjection(currentDatabase(), 'mt_rebuild', '_commit_order')
order by _block_number, _block_offset
settings max_threads = 1;

drop table mt_rebuild sync;
