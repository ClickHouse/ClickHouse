create table t(a UInt32, b String) engine=MergeTree order by ();

insert into t values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');

create view v as select * from t;

set enable_parallel_replicas = 1, parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

set enable_analyzer = 1, use_query_condition_cache = 1;

select * from t where a = 0;

select * from v where a = 0 settings log_comment = '03603_query_condition_cache_parallel_replicas_and_views';

system flush logs;

select throwIf(sum(ProfileEvents['QueryConditionCacheHits'] + ProfileEvents['QueryConditionCacheMisses']) = 0)
from system.query_log
where event_date >= yesterday() and event_time >= now() - INTERVAL '10 MINUTES' and log_comment = '03603_query_condition_cache_parallel_replicas_and_views' and current_database = currentDatabase() and type = 2
format Null;

drop view v;
drop table t;
