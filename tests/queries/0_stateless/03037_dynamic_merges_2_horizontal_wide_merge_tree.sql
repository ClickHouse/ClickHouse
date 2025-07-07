-- Tags: long, no-tsan, no-msan, no-ubsan, no-asan

set allow_experimental_dynamic_type = 1;

drop table if exists test;
create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, lock_acquire_timeout_for_background_operations=600;
system stop merges test;
insert into test select number, number from numbers(200000);
insert into test select number, 'str_' || toString(number) from numbers(200000, 200000);
insert into test select number, range(number % 10 + 1) from numbers(400000, 200000);
system start merges test;
optimize table test final;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
drop table test;
