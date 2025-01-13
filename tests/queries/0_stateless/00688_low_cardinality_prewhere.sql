drop table if exists lc_prewhere;
create table lc_prewhere (key UInt64, val UInt64, str StringWithDictionary, s String) engine = MergeTree order by key settings index_granularity = 8192;
insert into lc_prewhere select number, if(number < 10 or number > 8192 * 9, 1, 0), toString(number) as s, s from system.numbers limit 100000;
select sum(toUInt64(str)), sum(toUInt64(s)) from lc_prewhere prewhere val == 1;
drop table if exists lc_prewhere;
