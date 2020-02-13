drop table if exists lc_00931;
create table lc_00931 (key UInt64, value Array(LowCardinality(String))) engine = MergeTree order by key;
insert into lc_00931 select number, if(number < 10000 or number > 100000, [toString(number)], emptyArrayString()) from system.numbers limit 200000;
select * from lc_00931 where (key < 100 or key > 50000) and not has(value, toString(key)) and length(value) == 1 limit 10 settings max_block_size = 8192, max_threads = 1;

drop table if exists lc_00931;

