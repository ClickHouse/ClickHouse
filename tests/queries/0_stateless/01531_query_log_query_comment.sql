set log_queries=1;
set log_queries_min_type='QUERY_FINISH';

set enable_global_with_statement=0;
select /* test=01531, enable_global_with_statement=0 */ 2;
system flush logs query_log;
select count() from system.query_log
where event_date >= yesterday()
    and query like 'select /* test=01531, enable_global_with_statement=0 */ 2%'
    and current_database = currentDatabase()
    ;

set enable_global_with_statement=1;
select /* test=01531, enable_global_with_statement=1 */ 2;
system flush logs query_log;
select count() from system.query_log
where event_date >= yesterday()
    and query like 'select /* test=01531, enable_global_with_statement=1 */ 2%'
    and current_database = currentDatabase()
    ;
