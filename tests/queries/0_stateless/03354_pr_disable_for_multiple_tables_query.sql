drop table if exists X sync;
drop table if exists Y sync;
drop table if exists Z sync;

create table X (id Int32, x_a String, x_b Nullable(Int32)) engine ReplicatedMergeTree('/clickhouse/{database}/X', '1') order by id settings index_granularity=1;
create table Y (id Int32, y_a String, y_b Nullable(String)) engine ReplicatedMergeTree('/clickhouse/{database}/Y', '1') order by id settings index_granularity=1;

insert into X (id, x_a, x_b) select number, toString(number), -number from numbers(10000);
insert into X (id, x_a, x_b) values (1, 'l1', 1), (2, 'l2', 2), (2, 'l3', 3), (3, 'l4', 4);
insert into X (id, x_a) values      (4, 'l5'), (4, 'l6'), (5, 'l7'), (8, 'l8'), (9, 'l9');
insert into X (id, x_a, x_b) select number, toString(number), toString(-number) from numbers(10000);
insert into Y (id, y_a) values      (1, 'r1'), (1, 'r2'), (2, 'r3'), (3, 'r4'), (3, 'r5');
insert into Y (id, y_a, y_b) values (4, 'r6', 'nr6'), (6, 'r7', 'nr7'), (7, 'r8', 'nr8'), (9, 'r9', 'nr9');

-- A `FINAL`-supporting engine, for the `FINAL` probe below.
create table Z (id Int32, z_a String) engine ReplicatedReplacingMergeTree('/clickhouse/{database}/Z', '1') order by id settings index_granularity=1;
insert into Z (id, z_a) select number, toString(number) from numbers(1000);

set enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Pin the settings that the flaky/stress randomizer changes and that would otherwise alter the plan:
-- with automatic_parallel_replicas_mode=2 only statistics are collected and parallel replicas are not
-- actually used (so ReadFromRemoteParallelReplicas disappears from the plan), and
-- parallel_replicas_local_plan affects whether ReadFromRemoteParallelReplicas appears at all.
-- `serialize_query_plan` is turned on by the `distributed plan` checks; with a serialized plan the
-- subquery probes below do not spawn secondary queries at all, which would make their control run vacuous.
set automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1, serialize_query_plan = 0;

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
-- replicas inside `CreatingSets`. `EXPLAIN` does not print the plans of the set subqueries, so the probe
-- is `enable_parallel_replicas = 2` (refuse instead of silently falling back) together with a `FINAL`
-- read inside the subquery, which parallel replicas do not support: when the subquery is still planned
-- with parallel replicas the query is refused. With the setting enabled it must be refused, which is
-- also the control that the probe is not vacuous; with the setting disabled it must not be.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from (select * from X as s inner join Y as j on s.id = j.id where s.id in (select id from Z final))
    settings enable_parallel_replicas = 2, parallel_replicas_allow_in_with_subquery = 1; -- { serverError SUPPORT_IS_DISABLED }
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() > 0 from (select * from X as s inner join Y as j on s.id = j.id where s.id in (select id from Z final))
    settings enable_parallel_replicas = 2, parallel_replicas_allow_in_with_subquery = 1;

-- A materialized CTE is planned by yet another independent `Planner`, built from the CTE subquery's own
-- context in `addBuildSubqueriesForMaterializedCTEsIfNeeded` after the join kill switch has run, so the
-- switch must reach that context as well. The CTE is referenced twice, otherwise it is inlined and becomes
-- an ordinary subquery table expression. `EXPLAIN` does not print the CTE materialization plan either, so
-- the probe is the same `FINAL` refusal as for the `IN` subquery above.
set enable_materialized_cte = 1;
set parallel_replicas_for_queries_with_multiple_tables=1;
with a as materialized (select id from Z final)
    select count() > 0 from X as s inner join a as l on s.id = l.id inner join a as r on s.id = r.id
    settings enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
set parallel_replicas_for_queries_with_multiple_tables=0;
with a as materialized (select id from Z final)
    select count() > 0 from X as s inner join a as l on s.id = l.id inner join a as r on s.id = r.id
    settings enable_parallel_replicas = 2;
set enable_materialized_cte = 0;

-- The parallel-replicas compatibility checks of the planner (`parallel_replicas_allow_in_with_subquery`,
-- `additional_table_filters`, `FINAL`, ...) run before the join tree is planned, and with
-- `enable_parallel_replicas = 2` they throw instead of silently turning parallel replicas off.
-- The kill switch has to run before them: a query for which parallel replicas are already disabled by
-- the setting must simply be executed without them, not fail with a parallel-replicas-only exception.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from X as s inner join Y as j on s.id = j.id where s.id in (select id from Y)
    settings parallel_replicas_allow_in_with_subquery = 0, enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() > 0 from X as s inner join Y as j on s.id = j.id where s.id in (select id from Y)
    settings parallel_replicas_allow_in_with_subquery = 0, enable_parallel_replicas = 2;

set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from Z as s final inner join Y as j on s.id = j.id settings enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() > 0 from Z as s final inner join Y as j on s.id = j.id settings enable_parallel_replicas = 2;

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
