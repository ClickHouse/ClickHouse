-- Tags: no-parallel, no-replicated-database
-- Tag no-parallel: due to attaching to system.query_log
-- Tag no-replicated-database: Replicated database will has extra queries

-- Attach MV to system.query_log and check that writing query_log will not fail

set log_queries=1;

drop table if exists log_proxy_02572;
drop table if exists push_to_logs_proxy_mv_02572;

-- create log tables
system flush logs;
create table log_proxy_02572 as system.query_log engine=Distributed('test_shard_localhost', currentDatabase(), 'receiver_02572');
create materialized view push_to_logs_proxy_mv_02572 to log_proxy_02572 as select * from system.query_log;

select 1 format Null;
system flush logs;
system flush logs;

drop table log_proxy_02572;
drop table push_to_logs_proxy_mv_02572;

set log_queries=0;

system flush logs;
-- lower() to pass through clickhouse-test "exception" check
select replaceAll(query, '\n', '\\n'), lower(type::String), errorCodeToName(exception_code)
    from system.query_log
    where current_database = currentDatabase()
    order by event_time_microseconds
    format CSV;
