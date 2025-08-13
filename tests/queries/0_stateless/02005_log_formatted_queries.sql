set log_formatted_queries = 1;

select '02005_log_formatted_queries.sql' from system.one;
system flush logs;
select query, formatted_query from system.query_log where current_database = currentDatabase() and query = 'select \'02005_log_formatted_queries.sql\' from system.one;' and event_date >= yesterday() and event_time > now() - interval 5 minute;
