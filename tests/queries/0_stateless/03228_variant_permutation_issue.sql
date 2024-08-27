SET allow_experimental_variant_type = 1;

select 'test';
drop table if exists test;
create table test (id UInt32, data Variant(String), version UInt64) engine=ReplacingMergeTree(version) order by id;
insert into test values (1, NULL, 1), (2, 'bar', 1), (3, 'bar', 1);
insert into test select id, 'baz' as _data, version+1 as _version from test where id=2;
select * from test final WHERE data is not null format Null;
drop table test;
