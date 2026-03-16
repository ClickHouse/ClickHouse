-- Possible crash in case of mutations contains subquery, that will use
-- InterpreterSelectQuery() with only_analyze=true, which uses
-- getStorageSnapshotWithoutData(), and may crash in
-- getConditionSelectivityEstimator() since parts was nullptr

drop table if exists t0;
drop table if exists t1;

create table t0 (key Int) engine=MergeTree order by () settings auto_statistics_types='';
create table t1 (key Int) engine=MergeTree order by () settings auto_statistics_types='';
insert into t0 values (1);
insert into t1 values (1);

alter table t1 update key = 0 where 1 or not(not exists (select key from t0 where key > 0)) settings mutations_sync=2, allow_experimental_analyzer=0, query_plan_optimize_prewhere=0, query_plan_enable_optimizations=0, allow_statistics_optimize=1;
