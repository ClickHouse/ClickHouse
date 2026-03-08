-- Tags: zookeeper, no-replicated-database, no-parallel, no-object-storage
-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

drop table if exists x;

create table x(i int, index mm LOG2(i) type minmax granularity 1, projection p (select MAX(i))) engine ReplicatedMergeTree('/clickhouse/tables/{database}/x', 'r') order by i;

alter table x add index nn LOG2(i) type minmax granularity 1, add projection p2 (select MIN(i));

show create x;

select value from system.zookeeper WHERE name = 'metadata' and path = '/clickhouse/tables/'||currentDatabase()||'/x';

drop table x;
