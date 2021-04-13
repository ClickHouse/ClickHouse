set log_queries=1;
set log_queries_min_type='QUERY_FINISH';

set enable_global_with_statement=1;
select /* test=01531, enable_global_with_statement=0 */ 2;
system flush logs;
select count() from system.query_log where event_time >= now() - interval 5 minute and query = 'select /* test=01531, enable_global_with_statement=0 */ 2;\n';

set enable_global_with_statement=1;
select /* test=01531 enable_global_with_statement=1 */ 2;
system flush logs;
select count() from system.query_log where event_time >= now() - interval 5 minute and query = 'select /* test=01531 enable_global_with_statement=1 */ 2;\n';
