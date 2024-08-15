-- Tags: long, no-tsan, no-msan, no-ubsan, no-asan
set allow_experimental_dynamic_type=1;

drop table if exists test;
create table test (id UInt64, d Dynamic(max_types=3)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_columns_to_activate=10, index_granularity_bytes=10485760, index_granularity=8192, merge_max_block_size=8192, merge_max_block_size_bytes=10485760;

system stop merges test;
insert into test select number, number from numbers(100000);
insert into test select number, 'str_' || toString(number) from numbers(80000);
insert into test select number, range(number % 10 + 1) from numbers(70000);
insert into test select number, toDate(number) from numbers(60000);
insert into test select number, toDateTime(number) from numbers(50000);
insert into test select number, NULL from numbers(100000);

select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
system start merges test; optimize table test final;;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);

system stop merges test;
insert into test select number, map(number, number) from numbers(200000);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
system start merges test;
optimize table test final;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);

system stop merges test;
insert into test select number, tuple(number, number) from numbers(10000);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
system start merges test;
optimize table test final;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);

drop table test;
