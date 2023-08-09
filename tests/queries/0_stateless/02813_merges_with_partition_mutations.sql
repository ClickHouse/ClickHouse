DROP TABLE IF EXISTS multipart;
CREATE TABLE multipart
(
    `n` UInt32,
    `p` UInt32,
    `s` String
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY n;

INSERT INTO multipart select number, number%10, '' from numbers(18) settings max_partitions_per_insert_block=10;
optimize table multipart final;

alter table multipart update s='first' where p=7 SETTINGS mutations_sync = 2;
select 'global mutation';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');


alter table multipart update s='second' IN PARTITION 7 where p=7 SETTINGS mutations_sync = 2;
alter table multipart update s='third' where p=7 SETTINGS mutations_sync = 2;

select 'local, then global mutation';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');
optimize table multipart final;
select 'local, then global mutation, then optimize';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');

alter table multipart update s='fourth' where p=7 SETTINGS mutations_sync = 2;
alter table multipart update s='fifth' IN PARTITION 7 where p=7 SETTINGS mutations_sync = 2;

select 'global, then locl mutation';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');
optimize table multipart final;
select 'global, then locl mutation, then optimize';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');

insert into multipart values(42, 7, 'inserted - 1');
select 'insert';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');
alter table multipart update s='testing merge' where p=7 SETTINGS mutations_sync = 2;
optimize table multipart final;
select 'global mutation, optimize';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');

insert into multipart values(42, 7, 'inserted - 2');
select 'insert';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');
alter table multipart update s='testing merge' IN PARTITION 7 where p=7 SETTINGS mutations_sync = 2;
optimize table multipart final;
select 'local mutation, optimize';
select name from system.parts where table='multipart' and active=1 and part_id in ('7', '8');

drop table multipart
