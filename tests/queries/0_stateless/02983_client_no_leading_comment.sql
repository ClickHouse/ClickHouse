-- this comment should be skipped
select 1;

system flush logs;
select query from system.query_log where event_date >= yesterday() and current_database = currentDatabase() and type != 'QueryStart' and query = 'select 1;';
