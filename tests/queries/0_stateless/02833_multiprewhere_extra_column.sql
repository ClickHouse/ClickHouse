-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-object-storage

drop table if exists t_multi_prewhere;
drop row policy if exists policy_02834 on t_multi_prewhere;

create table t_multi_prewhere (a UInt64, b UInt64, c UInt8)
engine = MergeTree order by tuple()
settings min_bytes_for_wide_part = 0;

create row policy policy_02834 on t_multi_prewhere using a > 2000 as permissive to all;
insert into t_multi_prewhere select number, number, number from numbers(10000);

system drop mark cache;
select sum(b) from t_multi_prewhere prewhere a < 5000;

system flush logs query_log;

select ProfileEvents['FileOpen'] from system.query_log
where
    type = 'QueryFinish'
    and current_database = currentDatabase()
    and query ilike '%select sum(b) from t_multi_prewhere prewhere a < 5000%';

drop table if exists t_multi_prewhere;
drop row policy if exists policy_02834 on t_multi_prewhere;
