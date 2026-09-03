drop table if exists final_bug;
create table final_bug (x UInt64, y UInt8) engine = ReplacingMergeTree(y) order by x settings index_granularity = 8;
insert into final_bug select number % 10, 1 from numbers(1000);
insert into final_bug select number % 10, 1 from numbers(1000);
select x from final_bug final order by x settings max_threads=2, max_final_threads=2, max_block_size=8 format Null;
drop table if exists final_bug;