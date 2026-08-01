-- Tags: no-random-merge-tree-settings

CREATE TABLE tab (x UInt32, y UInt32, z UInt32) engine = MergeTree order by x settings min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
insert into tab select number, number, number from numbers(8129 * 123);

set enable_analyzer=1;
set prefer_localhost_replica=1;
set optimize_aggregation_in_order=0, optimize_read_in_order=0;

-- { echoOn }

select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x) where x = 42;
explain indexes=1 select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x) where x = 42;

select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by grouping sets ((x, z + 1), (x, z + 2))) where x = 42;
explain indexes=1 select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by grouping sets ((x, z + 1), (x, z + 2))) where x = 42;

select * from (select x, sum(y), z + 1 as q from remote('127.0.0.{1,2}', currentDatabase(), tab) group by grouping sets ((x, z + 1), (x, z + 2))) where q = 42;
explain indexes=1 select * from (select x, sum(y), z + 1 as q from remote('127.0.0.{1,2}', currentDatabase(), tab) group by grouping sets ((x, z + 1), (x, z + 2))) where q = 42;

set group_by_use_nulls=1;

select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x) where x = 42;
explain indexes=1 select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by x) where x = 42;

select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by grouping sets ((x, z + 1), (x, z + 2))) where x = 42;
explain indexes=1 select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab) group by grouping sets ((x, z + 1), (x, z + 2))) where x = 42;
