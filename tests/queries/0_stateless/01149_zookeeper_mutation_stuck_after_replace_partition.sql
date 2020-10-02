drop table if exists mt;
drop table if exists rmt;

create table mt (n UInt64, s String) engine = MergeTree partition by intDiv(n, 10) order by n;
insert into mt values (3, '3'), (4, '4');

create table rmt (n UInt64, s String) engine = ReplicatedMergeTree('/clickhouse/test_01149/rmt', 'r1') partition by intDiv(n, 10) order by n;
insert into rmt values (1,'1'), (2, '2');

select * from rmt;
select * from mt;
select table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') order by table, name;

alter table rmt update s = 's'||toString(n) where 1;    -- increment mutation version
select * from rmt;
alter table rmt replace partition '0' from mt;      -- (probably) removes parts incorrectly

--system restart replica rmt;  -- workaround

select table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') order by table, name;

alter table rmt drop column s;  -- hangs waiting for mutation of removed parts

-- see parts_to_do
select mutation_id, command, parts_to_do_names, parts_to_do, is_done from system.mutations where database=currentDatabase() and table='rmt';
select * from rmt;

drop table mt;
drop table rmt;
