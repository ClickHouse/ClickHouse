-- Random settings limits: index_granularity=(100, None)

set allow_experimental_dynamic_type=1;

drop table if exists test;
create table test (d Dynamic) engine=MergeTree order by tuple() settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
insert into test select number < 600000 ? number::Dynamic : ('str_' || number)::Dynamic from numbers(1000000);
alter table test modify column d Dynamic(max_types=1);
select count(), dynamicType(d), isDynamicElementInSharedData(d) from test group by dynamicType(d), isDynamicElementInSharedData(d) order by count();
drop table test;

