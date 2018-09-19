create table test_cardinality (x String) engine = MergeTree order by tuple();
insert into test_cardinality (x) select concat('v', toString(number)) from numbers(10);
alter table test_cardinality add column y LowCardinality(String);
select * from test_cardinality;

