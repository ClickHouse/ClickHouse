-- Cache is only for MergeTree
drop table if exists t_mem;
create table t_mem (key Int) engine=Memory();
insert into t_mem values (1);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mem';

-- MergeTree
drop table if exists t_mt;
-- { echoOn }
create table t_mt (key Int) engine=MergeTree() order by ();
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
insert into t_mt values (1);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
insert into t_mt values (2);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
alter table t_mt add column value String settings mutations_sync=2;
insert into t_mt values (10, '10');
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
insert into t_mt values (20, '20');
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
-- now let's try to remove ColumnsDescription with old structure
alter table t_mt detach part 'all_1_1_0';
alter table t_mt detach part 'all_2_2_0';
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
-- reattach
detach table t_mt;
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
attach table t_mt;
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_mt';
-- system.metrics
select value > 0 from system.metrics where metric = 'ColumnsDescriptionsCacheSize';
