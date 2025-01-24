-- Tags: zookeeper, no-random-merge-tree-settings, no-replicated-database

drop table if exists x1;
drop table if exists x2;

create table x1 (i Nullable(int)) engine ReplicatedMergeTree('/clickhouse/tables/{database}/x1', 'r1') order by i desc settings allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 10000, allow_experimental_reverse_key = 1;

create table x2 (i Nullable(int), j Nullable(int)) engine ReplicatedMergeTree('/clickhouse/tables/{database}/x2', 'r1') order by (i, j desc) settings allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 10000, allow_experimental_reverse_key = 1;

set allow_unrestricted_reads_from_keeper = 'true';

select value from system.zookeeper where path = '/clickhouse/tables/' || currentDatabase() || '/x1' and name = 'metadata';
select value from system.zookeeper where path = '/clickhouse/tables/' || currentDatabase() || '/x2' and name = 'metadata';

drop table x1;
drop table x2;
