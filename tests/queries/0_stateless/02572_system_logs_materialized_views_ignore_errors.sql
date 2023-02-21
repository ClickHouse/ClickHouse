-- Tags: no-parallel
-- Tag no-parallel: due to attaching to system.query_log

-- Attach MV to system.query_log and check that writing query_log will not fail

drop table if exists log_proxy_02572;
drop table if exists push_to_logs_proxy_mv_02572;

create table log_proxy_02572 as system.query_log engine=Distributed('test_shard_localhost', currentDatabase(), 'receiver_02572');
create materialized view push_to_logs_proxy_mv_02572 to log_proxy_02572 as select * from system.query_log;

set log_queries=1;
system flush logs;
system flush logs;

drop table log_proxy_02572;
drop table push_to_logs_proxy_mv_02572;
