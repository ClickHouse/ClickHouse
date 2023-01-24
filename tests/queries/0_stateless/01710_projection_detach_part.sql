-- Tags: no-s3-storage
set allow_experimental_projection_optimization = 1;

drop table if exists t;

create table t (i int, j int, projection x (select * order by j)) engine MergeTree partition by i order by i;

insert into t values (1, 2);

alter table t detach partition 1;

alter table t attach partition 1;

select count() from system.projection_parts where database = currentDatabase() and table = 't';

drop table t;
