-- Companion of `03354_pr_disable_for_multiple_tables_query`: that test checks the query plan of a
-- multi-table query, this one checks the parts of the query that are planned by an independent
-- `Planner` (prepared sets, materialized CTEs, correlated subqueries) and the legacy interpreter path.
-- The two are separate files because together they run too long in the flaky check.

drop table if exists X;
drop table if exists Y;
drop table if exists Z;

create table X (id Int32, x_a String, x_b Nullable(Int32)) engine MergeTree order by id settings index_granularity=1;
create table Y (id Int32, y_a String, y_b Nullable(String)) engine MergeTree order by id settings index_granularity=1;

insert into X (id, x_a, x_b) select number, toString(number), -number from numbers(100);
insert into X (id, x_a, x_b) values (1, 'l1', 1), (2, 'l2', 2), (2, 'l3', 3), (3, 'l4', 4);
insert into X (id, x_a) values      (4, 'l5'), (4, 'l6'), (5, 'l7'), (8, 'l8'), (9, 'l9');
insert into X (id, x_a, x_b) select number, toString(number), toString(-number) from numbers(100);
insert into Y (id, y_a) values      (1, 'r1'), (1, 'r2'), (2, 'r3'), (3, 'r4'), (3, 'r5');
insert into Y (id, y_a, y_b) values (4, 'r6', 'nr6'), (6, 'r7', 'nr7'), (7, 'r8', 'nr8'), (9, 'r9', 'nr9');

-- A `FINAL`-supporting engine, for the `FINAL` probe below.
create table Z (id Int32, z_a String) engine ReplacingMergeTree order by id settings index_granularity=1;
insert into Z (id, z_a) select number, toString(number) from numbers(100);

set enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Pin the settings that the flaky/stress randomizer changes and that would otherwise alter the plan:
-- with automatic_parallel_replicas_mode=2 only statistics are collected and parallel replicas are not
-- actually used (so ReadFromRemoteParallelReplicas disappears from the plan), and
-- parallel_replicas_local_plan affects whether ReadFromRemoteParallelReplicas appears at all.
-- `serialize_query_plan` is turned on by the `distributed plan` checks; with a serialized plan the
-- subquery probes below do not spawn secondary queries at all, which would make their control run vacuous.
set automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1, serialize_query_plan = 0;
-- Plain `MergeTree` tables keep the test out of Keeper, whose round trips dominated its run time.
set parallel_replicas_for_non_replicated_merge_tree = 1;
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

-- A correlated subquery is planned through `buildPlannerForCorrelatedSubquery` by yet another independent
-- `Planner`, built from the correlated subquery's own context, so the kill switch has to reach that context
-- as well. The analyzer's `DisableParallelReplicasPass` already turns parallel replicas off for a correlated
-- subquery and for the query that contains it, regardless of this setting, so no probe can observe the
-- correlated subquery itself being read with parallel replicas: the query must simply run for both values
-- of the setting.
set allow_experimental_correlated_subqueries = 1;
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from X as s inner join Y as j on s.id = j.id where exists (select 1 from Z final where Z.id = s.id)
    settings enable_parallel_replicas = 2;
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() > 0 from X as s inner join Y as j on s.id = j.id where exists (select 1 from Z final where Z.id = s.id)
    settings enable_parallel_replicas = 2;

-- That analyzer pass turns parallel replicas off only for the query nodes it leaves after having met the
-- correlated subquery, so a subquery table expression that precedes the correlated subquery in the query tree
-- keeps them on. With the setting disabled the kill switch has to reach that subquery as well, even though the
-- enclosing multi-table query already runs without parallel replicas because of the correlated subquery. The
-- probe is the same `FINAL` refusal: with the setting enabled the query is refused (the control that the probe
-- is not vacuous), with the setting disabled it must run.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from (select id from Z final) as s inner join Y as j on s.id = j.id where exists (select 1 from X where X.id = s.id)
    settings enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() > 0 from (select id from Z final) as s inner join Y as j on s.id = j.id where exists (select 1 from X where X.id = s.id)
    settings enable_parallel_replicas = 2;

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

-- On the legacy path the kill switch must run before `JoinedTables` is constructed: `JoinedTables` captures
-- its own copy of the context, and the legacy subquery paths (`JoinedTables::makeLeftTableSubquery`,
-- `JoinedTables::rewriteDistributedInAndJoins`) keep using it, so a switch applied afterwards would leave a
-- subquery-backed `JOIN` planned with parallel replicas still enabled. The probe is the same `FINAL` refusal
-- as above: with the setting enabled the query is refused (the control that the probe is not vacuous), with
-- the setting disabled it must simply run without parallel replicas.
set parallel_replicas_for_queries_with_multiple_tables=1;
select count() > 0 from (select id from Z final) as s inner join Y as j on s.id = j.id
    settings enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
set parallel_replicas_for_queries_with_multiple_tables=0;
select count() > 0 from (select id from Z final) as s inner join Y as j on s.id = j.id
    settings enable_parallel_replicas = 2;


drop table X;
drop table Y;
drop table Z;
