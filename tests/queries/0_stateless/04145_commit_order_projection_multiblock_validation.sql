-- Tags: no-parallel-replicas

set enable_analyzer = 1;
set mutations_sync = 2;

drop table if exists projection_bn_column sync;
CREATE TABLE projection_bn_column(a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1, min_bytes_for_wide_part=0, min_parts_to_merge_at_once=100;

-- Insert 2 parts and merge them into a level-1 multi-block part.
insert into projection_bn_column values (3, 30) (1, 10) (2, 20);
insert into projection_bn_column values (6, 60) (4, 40) (5, 50);
optimize table projection_bn_column final;

-- Insert a 3rd part (level-0) alongside the merged part.
insert into projection_bn_column values (9, 90) (7, 70) (8, 80);

-- Start
alter table projection_bn_column add projection p (
    select *, _block_number, _block_offset
    order by _block_number, _block_offset
);

-- Materialization should use parent part's block numbers
alter table projection_bn_column materialize projection p;
select 'after materialize';
select t.a            as a,
       t._block_number as parent_bn,
       p._block_number as projection_bn,
       t._block_offset as parent_bo,
       p._block_offset as projection_bo
from projection_bn_column as t
join mergeTreeProjection(currentDatabase(), 'projection_bn_column', 'p') as p
using (a)
order by a
format TSVWithNames;

-- Merge should use parent part's block numbers
optimize table projection_bn_column final;
select 'after merge';
select t.a            as a,
       t._block_number as parent_bn,
       p._block_number as projection_bn,
       t._block_offset as parent_bo,
       p._block_offset as projection_bo
from projection_bn_column as t
join mergeTreeProjection(currentDatabase(), 'projection_bn_column', 'p') as p
using (a)
order by a
format TSVWithNames;

-- Mutation should use parent part's block numbers
alter table projection_bn_column update b = b + 100 where a > 0;
select 'after mutation';
select t.a            as a,
       t._block_number as parent_bn,
       p._block_number as projection_bn,
       t._block_offset as parent_bo,
       p._block_offset as projection_bo
from projection_bn_column as t
join mergeTreeProjection(currentDatabase(), 'projection_bn_column', 'p') as p
using (a)
order by a
format TSVWithNames;

drop table projection_bn_column sync;
