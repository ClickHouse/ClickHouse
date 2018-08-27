set allow_experimental_low_cardinality_type = 1;
drop table if exists test.lc_dict_reading;
create table test.lc_dict_reading (val UInt64, str StringWithDictionary, pat String) engine = MergeTree order by val;
insert into test.lc_dict_reading select number, if(number < 8192 * 4, number % 100, number) as s, s from system.numbers limit 1000000;
select sum(toUInt64(str)), sum(toUInt64(pat)) from test.lc_dict_reading where val < 8129 or val > 8192 * 4;
drop table if exists test.lc_dict_reading;

