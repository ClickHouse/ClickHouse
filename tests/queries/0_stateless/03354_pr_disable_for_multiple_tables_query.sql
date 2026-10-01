-- Checks that `parallel_replicas_for_queries_with_multiple_tables` reaches every place that plans a
-- part of a multi-table query, by looking for `ReadFromRemoteParallelReplicas` in the query plan.
-- The probes that cannot be seen in `EXPLAIN` live in `05137_pr_disable_for_multiple_tables_execution`.

drop table if exists X;
drop table if exists Y;

create table X (id Int32, x_a String, x_b Nullable(Int32)) engine MergeTree order by id settings index_granularity=1;
create table Y (id Int32, y_a String, y_b Nullable(String)) engine MergeTree order by id settings index_granularity=1;

insert into X (id, x_a, x_b) select number, toString(number), -number from numbers(100);
insert into X (id, x_a, x_b) values (1, 'l1', 1), (2, 'l2', 2), (2, 'l3', 3), (3, 'l4', 4);
insert into X (id, x_a) values      (4, 'l5'), (4, 'l6'), (5, 'l7'), (8, 'l8'), (9, 'l9');
insert into X (id, x_a, x_b) select number, toString(number), toString(-number) from numbers(100);
insert into Y (id, y_a) values      (1, 'r1'), (1, 'r2'), (2, 'r3'), (3, 'r4'), (3, 'r5');
insert into Y (id, y_a, y_b) values (4, 'r6', 'nr6'), (6, 'r7', 'nr7'), (7, 'r8', 'nr8'), (9, 'r9', 'nr9');

set enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Pin the settings that the flaky/stress randomizer changes and that would otherwise alter the plan:
-- with automatic_parallel_replicas_mode=2 only statistics are collected and parallel replicas are not
-- actually used (so ReadFromRemoteParallelReplicas disappears from the plan), and
-- parallel_replicas_local_plan affects whether ReadFromRemoteParallelReplicas appears at all.
-- `serialize_query_plan` is turned on by the `distributed plan` checks and changes the printed plan.
set automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1, serialize_query_plan = 0;
-- Plain `MergeTree` tables keep the test out of Keeper, whose round trips dominated its run time.
set parallel_replicas_for_non_replicated_merge_tree = 1;

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

-- ARRAY JOIN over a single table is not a multi-table query, so parallel replicas must stay enabled
-- regardless of the setting. The check must count actual joins, not the table expressions stack size,
-- which also includes the ARRAY_JOIN node.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select id, n from X array join [1, 2] as n) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select id, n from X array join [1, 2] as n) where explain ilike '%ReadFromRemoteParallelReplicas%';

-- A `UNION` table expression nested under a `JOIN` is planned branch by branch, and every branch is
-- planned by an independent `Planner` built from the branch's own context, so disabling the setting
-- must reach the branch contexts as well, not only the `UnionNode` context.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select id from X union all select id from X) as u inner join Y as j on u.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select id from X union all select id from X) as u inner join Y as j on u.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

-- The same for a `UNION` nested inside another `UNION`, which requires the descent to be recursive.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (explain select * from (select id from X union all (select id from X union all select id from X)) as u inner join Y as j on u.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select * from (select id from X union all (select id from X union all select id from X)) as u inner join Y as j on u.id = j.id) where explain ilike '%ReadFromRemoteParallelReplicas%';

drop table X;
drop table Y;
