-- { echo ON }

drop table if exists test;

-- disable merge
create table test (i int, j int, projection p (select *, _part_offset order by j)) engine MergeTree order by i settings index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

-- make 5 parts
insert into test select number, 10 - number from numbers(5);
insert into test select number, 10 - number from numbers(5);
insert into test select number, 10 - number from numbers(5);
insert into test select number, 10 - number from numbers(5);
insert into test select number, 10 - number from numbers(5);

-- verify _part_starting_offset and _part_offset in parent part and projection
select _part, _part_starting_offset, _part_offset from test order by all;
select _part, _part_starting_offset, _part_offset from test where j = 8 order by all;

-- make sure key analysis works correctly
select *, _part_starting_offset + _part_offset from test where _part_starting_offset + _part_offset = 8 settings parallel_replicas_local_plan = 0, max_rows_to_read = 1;
select *, _part_offset + _part_starting_offset from test where _part_offset + _part_starting_offset = 8 settings parallel_replicas_local_plan = 0, max_rows_to_read = 1;

-- from fuzzer
select * from test prewhere 8 = (_part_offset + _part_starting_offset) where 8 = (_part_offset + _part_starting_offset) settings parallel_replicas_local_plan = 0, max_rows_to_read = 1;
select * from test prewhere (8 = (_part_starting_offset * _part_offset)) AND 3 WHERE 8 = (_part_starting_offset + _part_offset);

drop table test;
