-- TODO:
-- - add tests for other query types
-- - check some columns (i.e. read_rows/result_rows/written_rows/...)
set log_queries=1;
set log_queries_min_type='QUERY_START';

select '127.1';
select 'query from 127.1', * from remote('127.1', system.one) format Null;
system flush logs;
select type, count()
    from system.query_log
    where current_database = currentDatabase() and event_date = today() and query ilike 'select%query from 127.1%one%' and query not like '%query_log%'
    group by type
    order by type;

select '127.{1,2}';
select 'query from 127.{1,2}', * from remote('127.{1,2}', system.one) format Null;
system flush logs;
-- current_database does not passed across the nodes, so query a little bit more complex
with (
    select distinct initial_query_id
    from system.query_log
    where current_database = currentDatabase() and query ilike 'select%query from 127.{1,2}%one%' and query not like '%query_log%'
) as initial_query_id_
select type, count()
    from system.query_log
    where initial_query_id = initial_query_id_
    group by type
    order by type;
