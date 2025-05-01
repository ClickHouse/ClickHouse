-- Tags: no-replicated-database

create table mut (n int, m int, k int) engine=ReplicatedMergeTree('/test/02441/{database}/mut', '1') order by n;
set insert_keeper_fault_injection_probability=0;
insert into mut values (1, 2, 3), (10, 20, 30);

system stop merges mut;
alter table mut delete where n = 10;

-- a funny way to wait for a MUTATE_PART to be assigned
select sleepEachRow(2) from url('http://localhost:8123/?param_tries={1..10}&query=' || encodeURLComponent(
            'select 1 where ''MUTATE_PART'' not in (select type from system.replication_queue where database=''' || currentDatabase() || ''' and table=''mut'')'
    ), 'LineAsString', 's String') settings max_threads=1 format Null;

alter table mut drop column k settings alter_sync=0;
system sync replica mut pull;

-- a funny way to wait for ALTER_METADATA to disappear from the replication queue
select sleepEachRow(2) from url('http://localhost:8123/?param_tries={1..10}&query=' || encodeURLComponent(
    'select * from system.replication_queue where database=''' || currentDatabase() || ''' and table=''mut'' and type=''ALTER_METADATA'''
    ), 'LineAsString', 's String') settings max_threads=1 format Null;

select type, new_part_name, parts_to_merge from system.replication_queue where database=currentDatabase() and table='mut';
system start merges mut;
set receive_timeout=30;
system sync replica mut;
select * from mut;
