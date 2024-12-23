DROP TABLE IF EXISTS table1;

SYSTEM FLUSH LOGS;
select count() from (select * from system.query_log where has(databases, currentDatabase()) and tables[1] ilike '%table1' and query_kind = 'Drop' and current_database = currentDatabase());
select count() from (select * from system.query_log where has(databases, currentDatabase()) and tables[1] ilike '%table1' and query_kind = 'Drop' and current_database = currentDatabase()) settings enable_parallel_replicas=1, max_parallel_replicas=2, cluster_for_parallel_replicas='parallel_replicas';
