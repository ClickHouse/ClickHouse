-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug
drop table if exists tab_lc;
CREATE TABLE tab_lc (x UInt64, y LowCardinality(String)) engine = MergeTree order by x;
insert into tab_lc select number, toString(number % 10) from numbers(20000000);
optimize table tab_lc;
select count() from tab_lc where y == '0' settings local_filesystem_read_prefetch=1;
drop table if exists tab_lc;
