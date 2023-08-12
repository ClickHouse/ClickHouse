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
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');


alter table multipart update s='second' IN PARTITION 7 where p=7 SETTINGS mutations_sync = 2;
alter table multipart update s='third' where p=7 SETTINGS mutations_sync = 2;

select 'local, then global mutation';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');
optimize table multipart final;
select 'local, then global mutation, then optimize';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');

alter table multipart update s='fourth' where p=7 SETTINGS mutations_sync = 2;
alter table multipart update s='fifth' IN PARTITION 7 where p=7 SETTINGS mutations_sync = 2;

select 'global, then local mutation';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');
optimize table multipart final;
select 'global, then local mutation, then optimize';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');


alter table multipart update s='testing merge' where p=7 SETTINGS mutations_sync = 2;
select 'global mutation';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');

insert into multipart values(42, 8, 'inserted - 1');
select 'global mutation, insert';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');

optimize table multipart final;
select 'global mutation, insert, optimize';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');


alter table multipart update s='testing merge' IN PARTITION 7 where p=7 SETTINGS mutations_sync = 2;
select 'local mutation';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');

insert into multipart values(42, 8, 'inserted - 2');
select 'local mutation, insert';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');

optimize table multipart final;
select 'local mutation, insert, optimize';
select name from system.parts where table='multipart' and active=1 and (name LIKE '7\\_%' OR name LIKE '8\\_%');

drop table multipart;
