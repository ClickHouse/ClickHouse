-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug
drop table if exists tab_lc;
CREATE TABLE tab_lc (x UInt64, y LowCardinality(String)) engine = MergeTree order by x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into tab_lc select number, toString(number % 10) from numbers(20000000);
optimize table tab_lc;
SET max_rows_to_read = '21M';
select count() from tab_lc where y == '0' settings local_filesystem_read_prefetch=1;
drop table if exists tab_lc;
