drop table if exists mt;
drop table if exists rmt;


create table mt (`n` UInt64) engine = MergeTree partition by intDiv(n, 10) order by n;

insert into mt values (3), (4), (33), (44);

create table rmt (`n` UInt64) engine = ReplicatedMergeTree('/clickhouse/test_01149/rmt1', 'r1') partition by intDiv(n, 10) order by n;

insert into rmt values (1), (2);
select * from rmt;
select * from mt;

select database, table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') order by table, name;

alter table rmt replace partition '0' from mt;
select * from rmt;
select database, table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') order by table, name;

alter table rmt add column s String;
alter table rmt drop column s;
select * from rmt;
alter table rmt add column s String;
insert into rmt values (1, '1'), (2, '2');
alter table mt add column s String;
select * from rmt;
alter table rmt replace partition '0' from mt;      -- (probably) removes parts incorrectly

select database, table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') order by table, name;

alter table rmt drop column s;  -- hangs waiting for mutation of removed parts

-- see parts_to_do
select * from system.mutations where database=currentDatabase() and table='rmt';

select * from rmt;

drop table mt;
drop table rmt;
