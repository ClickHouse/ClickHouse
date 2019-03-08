-- basic test
drop table if exists test.simple;

create table test.simple (id UInt64,val SimpleAggregateFunction(sum,Double)) engine=AggregatingMergeTree order by id;
insert into test.simple select number,number from system.numbers limit 10;

select * from test.simple;
select * from test.simple final;
select toTypeName(val) from test.simple limit 1;

-- merge
insert into test.simple select number,number from system.numbers limit 10;

select * from test.simple final;

optimize table test.simple final;
select * from test.simple;

-- complex types
drop table if exists test.simple;

create table test.simple (id UInt64,nullable_str SimpleAggregateFunction(anyLast,Nullable(String)),low_str SimpleAggregateFunction(anyLast,LowCardinality(Nullable(String))),ip SimpleAggregateFunction(anyLast,IPv4)) engine=AggregatingMergeTree order by id;
insert into test.simple values(1,'1','1','1.1.1.1');
insert into test.simple values(1,null,'2','2.2.2.2');

select * from test.simple final;
select toTypeName(nullable_str),toTypeName(low_str),toTypeName(ip) from test.simple limit 1;
