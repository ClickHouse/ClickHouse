-- Tags: zookeeper, no-replicated-database, no-parallel, no-object-storage

drop table if exists x;

create table x(i int, index mm LOG2(i) type minmax granularity 1, projection p (select MAX(i))) engine ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r') order by i;

alter table x add index nn LOG2(i) type minmax granularity 1, add projection p2 (select MIN(i));

show create x;

select value from system.zookeeper WHERE name = 'metadata' and path = '/clickhouse/tables/'||currentDatabase()||'/x';

drop table x;
