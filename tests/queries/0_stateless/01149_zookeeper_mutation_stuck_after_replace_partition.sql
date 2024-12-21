-- Tags: zookeeper

SET insert_keeper_fault_injection_probability=0; -- disable fault injection; part ids are non-deterministic in case of insert retries

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

-- Example test case
INSERT INTO test_table PARTITION p1 VALUES (1, 'a');
ALTER TABLE test_table REPLACE PARTITION p1 FROM source_table;
-- ...additional test cases...

-- Test stale replica handling
SET replica_is_active_node = 0;
INSERT INTO rmt VALUES (5, '5');
ALTER TABLE rmt REPLACE PARTITION '0' FROM mt;
SET replica_is_active_node = 1;
SELECT * FROM rmt ORDER BY n;

-- Test complete partition copying
INSERT INTO mt VALUES (6, '6'), (7, '7');
ALTER TABLE rmt REPLACE PARTITION '0' FROM mt;
SELECT count(*) FROM rmt WHERE n IN (6, 7);

-- Test mutation versioning
SET mutations_sync = 2;
ALTER TABLE rmt UPDATE s = concat(s, '_updated') WHERE 1;
ALTER TABLE rmt REPLACE PARTITION '0' FROM mt;
SELECT s FROM rmt WHERE n IN (6, 7);

-- Test part uniqueness 
INSERT INTO mt VALUES (8, '8');
ALTER TABLE rmt REPLACE PARTITION '0' FROM mt;
SELECT count(*) FROM rmt WHERE n = 8;

drop table mt;
drop table rmt sync;
