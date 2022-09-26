drop table if exists t_index;
create table t_index(a int, b String) engine=MergeTree() order by a;

create index i_a on t_index(a) TYPE minmax GRANULARITY 4;
create index if not exists i_a on t_index(a) TYPE minmax GRANULARITY 2;

create index i_b on t_index(b) TYPE bloom_filter GRANULARITY 2;

show create table t_index;
select table, name, type, expr, granularity from system.data_skipping_indices where database = currentDatabase() and table = 't_index'; 

drop index i_a on t_index;
drop index if exists i_a on t_index;

select table, name, type, expr, granularity from system.data_skipping_indices where database = currentDatabase() and table = 't_index'; 

drop table t_index;
