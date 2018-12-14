set allow_experimental_low_cardinality_type = 1;
drop table if exists test.lc_prewhere;
create table test.lc_prewhere (key UInt64, val UInt64, str StringWithDictionary, s String) engine = MergeTree order by key settings index_granularity = 8192;
insert into test.lc_prewhere select number, if(number < 10 or number > 8192 * 9, 1, 0), toString(number) as s, s from system.numbers limit 100000;
select sum(toUInt64(str)), sum(toUInt64(s)) from test.lc_prewhere prewhere val == 1;
drop table if exists test.lc_prewhere;
