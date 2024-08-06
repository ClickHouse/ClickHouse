set allow_experimental_variant_type=1;
drop table if exists test;
create table test (id UInt64, v Variant(UInt64, String)) engine=MergeTree order by (id, v);
insert into test values (1, 1), (1, 'str_1'), (1, 2), (1, 'str_2');
select * from test;
drop table test;

