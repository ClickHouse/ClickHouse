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

-- Pin the settings that the flaky/stress randomizer changes and that would otherwise alter the plan:
-- with automatic_parallel_replicas_mode=2 only statistics are collected and parallel replicas are not
-- actually used (so ReadFromRemoteParallelReplicas disappears from the plan), and
-- parallel_replicas_local_plan affects whether ReadFromRemoteParallelReplicas appears at all.
set automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

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

-- An `IN` subquery is collected into a prepared set before the join kill switch runs, and it is later
-- planned by an independent `Planner` built from the subquery's own context, so the switch must be
-- propagated into the prepared-set subqueries as well, or the set would still be built with parallel
-- replicas inside `CreatingSets`. `EXPLAIN` does not print the plans of the set subqueries, so the
-- check is on the secondary queries the parallel replicas protocol spawns: there must be none when
-- the setting is disabled, and, as a control that the probe is not vacuous, some when it is enabled.
-- The `log_comment` is propagated to the secondary queries, and they are counted by `QueryStart`
-- because a secondary query may legitimately end with `ExceptionWhileProcessing` when the initiator
-- has already got the whole result and resets the connections.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() from (select * from X as s inner join Y as j on s.id = j.id where s.id in (select id from Y)) settings log_comment='03354_in_subquery_kill_switch_on' format Null;
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (select * from X as s inner join Y as j on s.id = j.id where s.id in (select id from Y)) settings log_comment='03354_in_subquery_kill_switch_off' format Null;
system flush logs query_log;
select count() > 0 from system.query_log
    where type = 'QueryStart' and not is_initial_query and event_date >= yesterday()
        and (current_database = currentDatabase() or has(databases, currentDatabase()))
        and log_comment = '03354_in_subquery_kill_switch_on';
select count() from system.query_log
    where type = 'QueryStart' and not is_initial_query and event_date >= yesterday()
        and (current_database = currentDatabase() or has(databases, currentDatabase()))
        and log_comment = '03354_in_subquery_kill_switch_off';

-- The legacy (pre-analyzer) interpreter must respect the setting as well: with
-- parallel_replicas_only_with_analyzer = 0 task-based parallel replicas are allowed on that path,
-- and the kill switch is applied in InterpreterSelectQuery before the storage read.
set enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0;
-- On the legacy path a JOIN can use parallel replicas only after the predicate optimizer has rewritten
-- the joined table into a subquery (`GlobalSubqueriesMatcher`: JOIN with parallel replicas is only
-- supported with subqueries), so pin `enable_optimize_predicate_expression` against the randomizer.
set enable_optimize_predicate_expression = 1;
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from (explain select X.*, Y.* from X inner join Y on X.id = Y.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() from (explain select X.*, Y.* from X inner join Y on X.id = Y.id) where explain ilike '%ReadFromRemoteParallelReplicas%';
-- A single-table query is not affected by the setting on the legacy path either.
select count() > 0 from (explain select * from X) where explain ilike '%ReadFromRemoteParallelReplicas%';

-- drop table X sync;
-- drop table Y sync;
