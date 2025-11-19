drop table if exists test_materialize;
create table test_materialize (part Int, key Int, value String) engine=MergeTree() partition by part order by key settings index_granularity=100, use_const_adaptive_granularity=false, enable_index_granularity_compression=false, min_bytes_for_wide_part=0;
insert into test_materialize select intDiv(number, 5000), number, repeat('a', number) from numbers(10e3) settings max_block_size=10, min_insert_block_size_rows=10000;

-- { echoOn }
select partition_id, rows, index_granularity_bytes_in_memory_allocated>25 from system.parts where database = currentDatabase() and table = 'test_materialize' and active order by 1;
alter table test_materialize modify setting use_const_adaptive_granularity;
alter table test_materialize add column new_value String;
alter table test_materialize delete where new_value != '';
alter table test_materialize rewrite parts settings mutations_sync=2;
select partition_id, rows, index_granularity_bytes_in_memory_allocated from system.parts where database = currentDatabase() and table = 'test_materialize' and active order by 1;

alter table test_materialize modify setting use_const_adaptive_granularity=0;
alter table test_materialize rewrite parts;
alter table test_materialize delete where (key % 2) == 0 settings mutations_sync=2;
select partition_id, rows, index_granularity_bytes_in_memory_allocated>25 from system.parts where database = currentDatabase() and table = 'test_materialize' and active order by 1;

select * from system.mutations where database = currentDatabase() and not is_done format Vertical;
