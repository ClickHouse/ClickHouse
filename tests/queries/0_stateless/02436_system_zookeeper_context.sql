DROP TABLE IF EXISTS mt;
create table mt (n int, s String) engine=MergeTree order by n;
insert into mt values (1, '');
set allow_nondeterministic_mutations=1;
alter table mt update s = (select toString(groupArray((*,))) from system.zookeeper where path='/') where n=1 settings mutations_sync=2;
select distinct n from mt;
DROP TABLE mt;
