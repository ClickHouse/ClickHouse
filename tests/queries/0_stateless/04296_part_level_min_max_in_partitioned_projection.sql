CREATE TABLE t (
  p UInt64,
  a UInt64,
  PROJECTION commit_order INDEX * TYPE commit_order,
)
engine=MergeTree
partition by p
order by a
settings enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1, part_minmax_index_columns = 'with_block_number_offset', add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1, index_granularity = 8192;

insert into t select 1, 1;
insert into t select 1, 2;
insert into t select 1, 3;
insert into t select 1, 4;
insert into t select 1, 5;
optimize table t final;

select 'Found' from t where _partition_id = '1' and _block_number = 3 order by (_block_number, _block_offset);
