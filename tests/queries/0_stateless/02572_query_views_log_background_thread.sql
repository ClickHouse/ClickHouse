-- INSERT buffer_02572 -> data_02572 -> copy_02572
--                                   ^^
--                             push to system.query_views_log

drop table if exists buffer_02572;
drop table if exists data_02572;
drop table if exists copy_02572;
drop table if exists mv_02572;

create table copy_02572 (key Int) engine=Memory();
create table data_02572 (key Int) engine=Memory();
create table buffer_02572 (key Int) engine=Buffer(currentDatabase(), data_02572, 1,
    /* never direct flush for flush from background thread */
    /* min_time= */ 3, 3,
    1, 1e9,
    1, 1e9);
create materialized view mv_02572 to copy_02572 as select * from data_02572;

-- { echoOn }
insert into buffer_02572 values (1);
-- ensure that the flush was not direct
select * from data_02572;
select * from copy_02572;
-- we cannot use OPTIMIZE, this will attach query context, so let's wait
SET function_sleep_max_microseconds_per_block = 6000000;
select sleepEachRow(1) from numbers(3*2) format Null;
select * from data_02572;
select * from copy_02572;

system flush logs;
select count() > 0, lower(status::String), errorCodeToName(exception_code)
    from system.query_views_log where
    view_name = concatWithSeparator('.', currentDatabase(), 'mv_02572') and
    view_target = concatWithSeparator('.', currentDatabase(), 'copy_02572')
    group by 2, 3
;
