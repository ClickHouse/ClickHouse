-- Tags: no-parallel, long, no-debug, no-tsan, no-msan, no-asan
SET max_rows_to_read = 0;

create table data_02344 (key Int) engine=Null;
-- 3e9 rows is enough to fill the socket buffer and cause INSERT hung.
insert into function remote('127.1', currentDatabase(), data_02344) select number from numbers(3e9) settings prefer_localhost_replica=0;
