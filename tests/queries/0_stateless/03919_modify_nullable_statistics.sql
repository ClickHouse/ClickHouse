drop table if exists t_stat_nullable_1 sync;
drop table if exists t_stat_nullable_2 sync;

create table t_stat_nullable_1(A Int64, B String, C String)
Engine = ReplicatedMergeTree('/clickhouse/{database}/tables/t_stat_nullable', '1')
order by A
SETTINGS auto_statistics_types = 'uniq';

create table t_stat_nullable_2( A Int64, B String, C String)
Engine = ReplicatedMergeTree('/clickhouse/{database}/tables/t_stat_nullable', '2')
order by A
SETTINGS auto_statistics_types = 'uniq';

set mutations_sync = 2;

insert into t_stat_nullable_1 values(1, 'one', 'test');
system sync replica t_stat_nullable_2;

select table, column, type, statistics, estimates.cardinality
from system.parts_columns
where database = currentDatabase() and table like 't_stat_nullable%' and column = 'B' and active;

alter table t_stat_nullable_1 detach partition tuple();
system sync replica t_stat_nullable_2;

alter table t_stat_nullable_1 modify column B Nullable(String);
system sync replica t_stat_nullable_2;

alter table t_stat_nullable_1 attach partition tuple();
system sync replica t_stat_nullable_2;

select table, column, type, statistics, estimates.cardinality
from system.parts_columns
where database = currentDatabase() and table like 't_stat_nullable%' and column = 'B' and active;

alter table t_stat_nullable_1 update B = 'test1' where 1;
system sync replica t_stat_nullable_2;

select * from t_stat_nullable_1;
select * from t_stat_nullable_2;

select table, column, type, statistics, estimates.cardinality
from system.parts_columns
where database = currentDatabase() and table like 't_stat_nullable%' and column = 'B' and active;

drop table if exists t_stat_nullable_1 sync;
drop table if exists t_stat_nullable_2 sync;
