-- Tags: no-replicated-database, no-parallel
-- Tag no-parallel: attaches a materialized view directly on the server-wide `system.query_log`
--  table, so every concurrent test's query gets pushed through this test's (possibly failing)
--  `push_to_logs_proxy_mv_02572` while it exists.
-- Tag no-replicated-database: Replicated database will has extra queries

-- Attach MV to system.query_log and check that writing query_log will not fail

set log_queries=1;

drop table if exists log_proxy_02572;
drop table if exists push_to_logs_proxy_mv_02572;

-- create log tables
system flush logs query_log;
create table log_proxy_02572 as system.query_log engine=Distributed('test_shard_localhost', currentDatabase(), 'receiver_02572');
create materialized view push_to_logs_proxy_mv_02572 to log_proxy_02572 as select * from system.query_log;

select 1 format Null;
system flush logs query_log;
system flush logs query_log;

drop table log_proxy_02572;
drop table push_to_logs_proxy_mv_02572;

set log_queries=0;

system flush logs query_log;
-- lower() to pass through clickhouse-test "exception" check
select replaceAll(query, '\n', '\\n'), lower(type::String), errorCodeToName(exception_code)
    from system.query_log
    where event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    order by event_time_microseconds
    format CSV;
