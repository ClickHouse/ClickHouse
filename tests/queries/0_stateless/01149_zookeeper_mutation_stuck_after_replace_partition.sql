-- Tags: zookeeper

set send_logs_level='error';
drop table if exists mt;
drop table if exists rmt sync;

create table mt (n UInt64, s String) engine = MergeTree partition by intDiv(n, 10) order by n;
insert into mt values (3, '3'), (4, '4');

create table rmt (n UInt64, s String) engine = ReplicatedMergeTree('/clickhouse/test_01149_{database}/rmt', 'r1') partition by intDiv(n, 10) order by n;
insert into rmt values (1, '1'), (2, '2');

select * from rmt;
select * from mt;
select table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') and active=1 order by table, name;

SET mutations_sync = 1;
alter table rmt update s = 's'||toString(n) where 1;

select * from rmt;
alter table rmt replace partition '0' from mt;

system sync replica rmt;

select table, partition_id, name, rows from system.parts where database=currentDatabase() and table in ('mt', 'rmt') and active=1 order by table, name;

alter table rmt drop column s;

select mutation_id, command, parts_to_do_names, parts_to_do, is_done from system.mutations where database=currentDatabase() and table='rmt';
select * from rmt;

drop table rmt sync;

set replication_alter_partitions_sync=0;
create table rmt (n UInt64, s String) engine = ReplicatedMergeTree('/clickhouse/test_01149_{database}/rmt', 'r1') partition by intDiv(n, 10) order by n;
insert into rmt values (1,'1'), (2, '2');

alter table rmt update s = 's'||toString(n) where 1;
alter table rmt drop partition '0';

set replication_alter_partitions_sync=1;
alter table rmt drop column s;

drop table mt;
drop table rmt sync;
