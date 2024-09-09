set allow_experimental_dynamic_type = 1;
set max_block_size = 1000;

drop table if exists test;

create table test (d Dynamic) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, NULL::Dynamic(max_types=2), number < 3000, range(number % 5)::Dynamic(max_types=2), number::Dynamic(max_types=2)) from numbers(1000000);
select distinct dynamicType(d) as type from test order by type;

drop table test;
create table test (d Dynamic(max_types=2)) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, NULL::Dynamic(max_types=2), number < 3000, range(number % 5)::Dynamic(max_types=2), number::Dynamic(max_types=2)) from numbers(1000000);
select distinct dynamicType(d) as type from test order by type;

truncate table test;
insert into test select multiIf(number < 1000, 'Str'::Dynamic(max_types=2), number < 3000, range(number % 5)::Dynamic(max_types=2), number::Dynamic(max_types=2)) from numbers(1000000);
select distinct dynamicType(d) as type from test order by type;

drop table test;

