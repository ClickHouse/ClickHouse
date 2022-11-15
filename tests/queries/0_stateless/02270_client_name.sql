select 1 settings log_queries=1, log_queries_min_type='QUERY_FINISH' format Null;
system flush logs;
select client_name from system.query_log where current_database = currentDatabase() and query like 'select 1%' format CSV;
