set prefer_localhost_replica=1;

drop table if exists data_02572;
drop table if exists proxy_02572;
drop table if exists push_to_proxy_mv_02572;
drop table if exists receiver_02572;

create table data_02572 (key Int) engine=Memory();

create table proxy_02572 (key Int) engine=Distributed('test_shard_localhost', currentDatabase(), 'receiver_02572');
-- ensure that insert fails
insert into proxy_02572 values (1); -- { serverError UNKNOWN_TABLE }

-- proxy data with MV
create materialized view push_to_proxy_mv_02572 to proxy_02572 as select * from data_02572;

-- { echoOn }
select * from data_02572 order by key;

insert into data_02572 settings materialized_views_ignore_errors=1 values (2);
select * from data_02572 order by key;
-- check system.query_views_log
system flush logs query_views_log;
-- lower(status) to pass through clickhouse-test "exception" check
select lower(status::String), errorCodeToName(exception_code)
from system.query_views_log where
    view_name = concatWithSeparator('.', currentDatabase(), 'push_to_proxy_mv_02572') and
    view_target = concatWithSeparator('.', currentDatabase(), 'proxy_02572')
    order by event_date, event_time
;

-- materialized_views_ignore_errors=0
insert into data_02572 values (1); -- { serverError UNKNOWN_TABLE }
select * from data_02572 order by key;

create table receiver_02572 as data_02572;

insert into data_02572 values (3);
select * from data_02572 order by key;
select * from receiver_02572 order by key;
