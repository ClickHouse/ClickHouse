-- Tags: long, no-tsan, no-msan, no-ubsan, no-asan

set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set allow_experimental_dynamic_type = 1;
set enable_named_columns_in_function_tuple = 0;

drop table if exists test;;
create table test (id UInt64, d Dynamic(max_types=3)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000, lock_acquire_timeout_for_background_operations=600;

system stop merges test;
insert into test select number, number from numbers(100000);
insert into test select number, tuple(if(number % 2 == 0, number, 'str_' || toString(number)))::Tuple(a Dynamic(max_types=3)) from numbers(100000);
insert into test select number, tuple(if(number % 3 == 0, toDate(number), range(number % 10)))::Tuple(a Dynamic(max_types=3)) from numbers(50000);

select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=3))`.a) as type from test group by type order by count(), type;
system start merges test;
optimize table test final;
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=3))`.a) as type from test group by type order by count(), type;

insert into test select number, tuple(if(number % 3 == 0, toDateTime(number), NULL))::Tuple(a Dynamic(max_types=3)) from numbers(50000);
insert into test select number, tuple(if(number % 2 == 0, tuple(number), NULL))::Tuple(a Dynamic(max_types=3)) from numbers(200000);

select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=3))`.a) as type from test group by type order by count(), type;
system start merges test;
optimize table test final;
select count(), dynamicType(d) || ':' || dynamicType(d.`Tuple(a Dynamic(max_types=3))`.a) as type from test group by type order by count(), type;

drop table test;
