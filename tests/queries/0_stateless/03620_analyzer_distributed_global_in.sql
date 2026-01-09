set serialize_query_plan = 0;
set enable_analyzer=1;
set enable_parallel_replicas=0;
set prefer_localhost_replica=1;

create table tab0 (x UInt32, y UInt32) engine = MergeTree order by x settings index_granularity=8192, min_bytes_for_wide_part=1e9, index_granularity_bytes=10e6;
insert into tab0 select number, number from numbers(8192 * 123);

-- { echo }
select sum(y) from (select * from remote('127.0.0.1', currentDatabase(), tab0)) where x in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.1', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.2', currentDatabase(), tab0)) where x in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.2', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.{2,3}', currentDatabase(), tab0)) where x in (select number + 42 from numbers(1));
select sum(y) from (select * from remote('127.0.0.{2,3}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select * from (explain indexes=1
    select sum(y) from (select * from remote('127.0.0.1', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.2', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);
system flush logs query_log;
-- SKIP: current_database = currentDatabase()
select normalizeQuery(replace(query, currentDatabase(), 'default')) from system.query_log where event_date >= yesterday() and log_comment like '%' || currentDatabase() || '%' and not is_initial_query and type != 'QueryStart' and query_kind = 'Select' order by event_time_microseconds;
