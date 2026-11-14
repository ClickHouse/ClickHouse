-- Tags: long
-- Random settings limits: index_granularity=(100, None)

set allow_experimental_dynamic_type = 1;
set max_block_size = 1000;

drop table if exists test;

create table test (d Dynamic) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, NULL::Dynamic(max_types=1), number < 3000, range(number % 5)::Dynamic(max_types=1), number::Dynamic(max_types=1)) from numbers(100000);
select '1';
select distinct dynamicType(d) as type, isDynamicElementInSharedData(d) as flag from test order by type;

drop table test;
create table test (d Dynamic(max_types=1)) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, NULL::Dynamic(max_types=1), number < 3000, range(number % 5)::Dynamic(max_types=1), number::Dynamic(max_types=1)) from numbers(100000);
select '2';
select distinct dynamicType(d) as type, isDynamicElementInSharedData(d) as flag from test order by type;

truncate table test;
insert into test select multiIf(number < 1000, 'Str'::Dynamic(max_types=1), number < 3000, range(number % 5)::Dynamic(max_types=1), number::Dynamic(max_types=1)) from numbers(100000);
select '3';
select distinct dynamicType(d) as type, isDynamicElementInSharedData(d) as flag from test order by type;

drop table test;

