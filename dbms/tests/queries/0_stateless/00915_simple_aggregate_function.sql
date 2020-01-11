-- basic test
drop table if exists simple;

create table simple (id UInt64,val SimpleAggregateFunction(sum,Double)) engine=AggregatingMergeTree order by id;
insert into simple select number,number from system.numbers limit 10;

select * from simple;
select * from simple final;
select toTypeName(val) from simple limit 1;

-- merge
insert into simple select number,number from system.numbers limit 10;

select * from simple final;

optimize table simple final;
select * from simple;

-- complex types
drop table if exists simple;

create table simple (id UInt64,nullable_str SimpleAggregateFunction(anyLast,Nullable(String)),low_str SimpleAggregateFunction(anyLast,LowCardinality(Nullable(String))),ip SimpleAggregateFunction(anyLast,IPv4)) engine=AggregatingMergeTree order by id;
insert into simple values(1,'1','1','1.1.1.1');
insert into simple values(1,null,'2','2.2.2.2');
-- String longer then MAX_SMALL_STRING_SIZE (actual string length is 100)
insert into simple values(10,'10','10','10.10.10.10');
insert into simple values(10,'2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222','20','20.20.20.20');

select * from simple final;
select toTypeName(nullable_str),toTypeName(low_str),toTypeName(ip) from simple limit 1;

optimize table simple final;

drop table simple;
