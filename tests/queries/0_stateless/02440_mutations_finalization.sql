-- Tags: no-parallel

create table mut (n int) engine=ReplicatedMergeTree('/test/02440/{database}/mut', '1') order by tuple();
set insert_keeper_fault_injection_probability=0;
insert into mut values (1);
system stop merges mut;
alter table mut update n = 2 where n = 1;
-- it will create MUTATE_PART entry, but will not execute it

system sync replica mut pull;
select mutation_id, command, parts_to_do_names, is_done from system.mutations where database=currentDatabase() and table='mut';

-- merges (and mutations) will start again after detach/attach, we need to avoid this somehow...
create table tmp (n int) engine=MergeTree order by tuple() settings index_granularity=1;
insert into tmp select * from numbers(1000);
alter table tmp update n = sleepEachRow(1) where 1;
select sleepEachRow(2) as higher_probablility_of_reproducing_the_issue format Null;

-- it will not execute MUTATE_PART, because another mutation is currently executing (in tmp)
alter table mut modify setting max_number_of_mutations_for_replica=1;
detach table mut;
attach table mut;

-- mutation should not be finished yet
select * from mut;
select mutation_id, command, parts_to_do_names, is_done from system.mutations where database=currentDatabase() and table='mut';

alter table mut modify setting max_number_of_mutations_for_replica=100;
system sync replica mut;

-- and now it should (is_done may be 0, but it's okay)
select * from mut;
select mutation_id, command, parts_to_do_names from system.mutations where database=currentDatabase() and table='mut';

drop table tmp; -- btw, it will check that mutation can be cancelled between blocks on shutdown
