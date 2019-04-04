drop table if exists test.lc;
create table test.lc (key UInt64, value Array(LowCardinality(String))) engine = MergeTree order by key;
insert into test.lc select number, if(number < 10000 or number > 100000, [toString(number)], emptyArrayString()) from system.numbers limit 200000;
select * from test.lc where (key < 100 or key > 50000) and not has(value, toString(key)) and length(value) == 1 limit 10 settings max_block_size = 8192, max_threads = 1;

drop table if exists test.lc;

