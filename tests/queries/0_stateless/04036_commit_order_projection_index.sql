-- Tags: no-parallel-replicas

set enable_analyzer = 1;

drop table if exists mt_commit_order_idx sync;

CREATE TABLE mt_commit_order_idx(
    a UInt64,
    b UInt64,
    projection commit_order index b type commit_order
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, allow_commit_order_projection=1, index_granularity=1;

insert into mt_commit_order_idx select rand(), rand() from numbers(10);
insert into mt_commit_order_idx select rand(), rand() from numbers(10);
insert into mt_commit_order_idx select rand(), rand() from numbers(10);
insert into mt_commit_order_idx select rand(), rand() from numbers(10);
optimize table mt_commit_order_idx final;

select 'reading all columns';
SELECT explain FROM (explain indexes=1, projections=1 select *, _block_number, _block_offset from mt_commit_order_idx where (_block_number, _block_offset) = (3, 6) settings optimize_use_projections=1, optimize_move_to_prewhere=1, query_plan_optimize_prewhere=1) WHERE explain NOT LIKE '%Condition%';

select '';
select 'reading indexed columns';
SELECT explain FROM (explain indexes=1, projections=1 select b, _block_number, _block_offset from mt_commit_order_idx where (_block_number, _block_offset) = (3, 6) settings optimize_use_projections=1, optimize_move_to_prewhere=1, query_plan_optimize_prewhere=1) WHERE explain NOT LIKE '%Condition%';

drop table if exists mt_commit_order_idx sync;
