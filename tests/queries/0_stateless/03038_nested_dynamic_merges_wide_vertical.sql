-- Tags: long,  no-tsan, no-msan, no-ubsan, no-asan
-- Random settings limits: index_granularity=(100, None); merge_max_block_size=(100, None)

set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set allow_experimental_dynamic_type = 1;
set enable_named_columns_in_function_tuple = 0;

drop table if exists test;;
create table test (id UInt64, d Dynamic(max_types=2)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1, lock_acquire_timeout_for_background_operations=600;

system stop merges test;
insert into test select number, number from numbers(100000);
insert into test select number, tuple(if(number % 3 == 0, number, 'str_' || toString(number)))::Tuple(a Dynamic(max_types=2)) from numbers(100000);
insert into test select number, tuple(if(number % 3 == 0, toDate(number), range(number % 10)))::Tuple(a Dynamic(max_types=2)) from numbers(50000);
insert into test select number, multiIf(number % 5 == 0, tuple(if(number % 3 == 0, toDateTime(number), toIPv4(number)))::Tuple(a Dynamic(max_types=2)), number % 5 == 1 or number % 5 == 2, number, 'str_' || number) from numbers(100000);

select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=2))`.a) as type, isDynamicElementInSharedData(d.`Tuple(a Dynamic(max_types=2))`.a) as flag from test group by type, flag order by count(), type;
system start merges test;
optimize table test final;
select '---------------------';
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=2))`.a) as type, isDynamicElementInSharedData(d.`Tuple(a Dynamic(max_types=2))`.a) as flag from test group by type, flag order by count(), type;

system stop merges test;
insert into test select number, tuple(if(number % 3 == 0, toDateTime(number), NULL))::Tuple(a Dynamic(max_types=2)) from numbers(50000);
insert into test select number, tuple(if(number % 2 == 0, tuple(number), NULL))::Tuple(a Dynamic(max_types=2)) from numbers(200000);

select '---------------------';
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=2))`.a) as type, isDynamicElementInSharedData(d.`Tuple(a Dynamic(max_types=2))`.a) as flag from test group by type, flag order by count(), type;
system start merges test;
optimize table test final;
select '---------------------';
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=2))`.a) as type, isDynamicElementInSharedData(d.`Tuple(a Dynamic(max_types=2))`.a) as flag from test group by type, flag order by count(), type;

system stop merges test;
insert into test select number, tuple(toDateTime(number))::Tuple(a Dynamic(max_types=2)) from numbers(40000);

select '---------------------';
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=2))`.a) as type, isDynamicElementInSharedData(d.`Tuple(a Dynamic(max_types=2))`.a) as flag from test group by type, flag order by count(), type;
system start merges test;
optimize table test final;
select '---------------------';
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=2))`.a) as type, isDynamicElementInSharedData(d.`Tuple(a Dynamic(max_types=2))`.a) as flag from test group by type, flag order by count(), type;

drop table test;
