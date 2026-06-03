-- Tags: no-shared-merge-tree
-- no-shared-merge-tree -- smt don't assign mutation with stop merges. so last `system sync replica mut` doesn't help.

create table mut (n int) engine=ReplicatedMergeTree('/test/02440/{database}/mut', '1') order by tuple();
set insert_keeper_fault_injection_probability=0;
insert into mut values (1);
system stop merges mut;
alter table mut update n = 2 where n = 1;
-- it will create MUTATE_PART entry, but will not execute it

system sync replica mut pull;
select mutation_id, command, parts_to_do_names, is_done from system.mutations where database=currentDatabase() and table='mut';

-- Block mutation execution deterministically after detach/attach re-enables
-- merges: require more free background-pool entries than will ever exist.
alter table mut modify setting number_of_free_entries_in_pool_to_execute_mutation=32;
detach table mut;
attach table mut;

-- mutation should not be finished yet
select * from mut;
select mutation_id, command, parts_to_do_names, is_done from system.mutations where database=currentDatabase() and table='mut';

alter table mut modify setting number_of_free_entries_in_pool_to_execute_mutation=0;
system sync replica mut strict;

-- and now it should be done
select * from mut;
select mutation_id, command, parts_to_do_names from system.mutations where database=currentDatabase() and table='mut';
