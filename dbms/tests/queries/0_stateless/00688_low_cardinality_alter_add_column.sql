drop table if exists test.cardinality;
create table test.cardinality (x String) engine = MergeTree order by tuple();
insert into test.cardinality (x) select concat('v', toString(number)) from numbers(10);
alter table test.cardinality add column y LowCardinality(String);
select * from test.cardinality;
drop table if exists test.cardinality;
