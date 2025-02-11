drop table if exists X sync;
drop table if exists Y sync;

create table X (id Int32, x_a String, x_b Nullable(Int32)) engine ReplicatedMergeTree('/clickhouse/{database}/X', '1') order by id settings index_granularity=1;
create table Y (id Int32, y_a String, y_b Nullable(String)) engine ReplicatedMergeTree('/clickhouse/{database}/Y', '1') order by id settings index_granularity=1;

insert into X (id, x_a, x_b) select number, toString(number), -number from numbers(10000);
insert into X (id, x_a, x_b) values (1, 'l1', 1), (2, 'l2', 2), (2, 'l3', 3), (3, 'l4', 4);
insert into X (id, x_a) values      (4, 'l5'), (4, 'l6'), (5, 'l7'), (8, 'l8'), (9, 'l9');
insert into X (id, x_a, x_b) select number, toString(number), toString(-number) from numbers(10000);
insert into Y (id, y_a) values      (1, 'r1'), (1, 'r2'), (2, 'r3'), (3, 'r4'), (3, 'r5');
insert into Y (id, y_a, y_b) values (4, 'r6', 'nr6'), (6, 'r7', 'nr7'), (7, 'r8', 'nr8'), (9, 'r9', 'nr9');

set enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select X.*, Y.* from X inner join Y on X.id = Y.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select X.*, Y.* from X inner join Y on X.id = Y.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select X.*, Y.* from X inner join Y on X.id = Y.id)) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select X.*, Y.* from X inner join Y on X.id = Y.id)) where explain ilike '%ReadFromRemoteParallelReplicas%';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select * from X) as s inner join Y as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select * from X) as s inner join Y as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from X as s inner join (select * from Y) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from X as s inner join (select * from Y) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select * from X) as s inner join (select * from Y) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select * from X) as s inner join (select * from Y) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select * from X) as s inner join (select * from Y order by id) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select * from X) as s inner join (select * from Y order by id) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select * from X order by id) as s inner join (select * from Y) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select * from X order by id) as s inner join (select * from Y) as j on s.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

-- drop table X sync;
-- drop table Y sync;
