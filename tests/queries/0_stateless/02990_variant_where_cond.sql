-- Tags: no-parallel-replicas

set allow_experimental_variant_type=1;

create table test (v Variant(String, UInt64)) engine=MergeTree ORDER BY tuple();
insert into test values (42), ('Hello'), (NULL);

select * from test where v = 'Hello';
select * from test where v = 42;
select * from test where v = 42::UInt64::Variant(String, UInt64);

drop table test;

