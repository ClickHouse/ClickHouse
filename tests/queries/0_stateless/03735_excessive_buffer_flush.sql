-- Note, this test uses sleep, but, it should not affect it's flakiness

set function_sleep_max_microseconds_per_block=5e9;

drop table if exists data;
create table data (key Int) engine=Null;

drop table if exists empty_buffer;
create table empty_buffer (key Int) engine=Buffer(currentDatabase(), data, 2, 2, 4, 100_000, 1_000_000, 10e9, 10e9, 3);
select sleep(5) format Null;
optimize table empty_buffer;
drop table empty_buffer;

drop table if exists empty_buffer_zero_time;
create table empty_buffer_zero_time (key Int) engine=Buffer(currentDatabase(), data, 2, 0, 0, 100_000, 1_000_000, 10e9, 10e9, 0);
select sleep(1) format Null;
optimize table empty_buffer_zero_time;
drop table empty_buffer_zero_time;

drop table if exists buffer_flush_by_min;
create table buffer_flush_by_min (key Int) engine=Buffer(currentDatabase(), data, 2, 2, 4, 100_000, 1_000_000, 0, 10e9, 3);
insert into buffer_flush_by_min select * from numbers(100_000 + 1);
select sleep(5) format Null;
drop table buffer_flush_by_min;

drop table if exists buffer_flush_by_max;
create table buffer_flush_by_max (key Int) engine=Buffer(currentDatabase(), data, 2, 2, 4, 100_000, 1_000_000, 0, 10e9);
insert into buffer_flush_by_max select * from numbers(1);
select sleep(5) format Null;
drop table buffer_flush_by_max;

drop table if exists buffer_flush_by_flush_time;
create table buffer_flush_by_flush_time (key Int) engine=Buffer(currentDatabase(), data, 2, 2, 4, 100_000, 1_000_000, 10e9, 10e9, 3);
insert into buffer_flush_by_flush_time values (1);
select sleep(5) format Null;
drop table buffer_flush_by_flush_time;

system flush logs text_log;
-- to avoid flakiness we only check that number of logs < 10, instead of some strict values
select extractAll(logger_name, 'StorageBuffer \\([^.]+\\.([^)]+)\\)')[1] as table_name, max2(count(), 10) from system.text_log where logger_name LIKE format('%StorageBuffer ({}.%', currentDatabase()) group by 1 order by 1;
