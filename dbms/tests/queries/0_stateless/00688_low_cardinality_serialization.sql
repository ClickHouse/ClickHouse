set allow_experimental_low_cardinality_type = 1;

select 'NativeBlockInputStream';
select toTypeName(dict), dict, lowCardinalityIndexes(dict), lowCardinalityKeys(dict) from (select '123_' || toLowCardinality(v) as dict from (select arrayJoin(['a', 'bb', '', 'a', 'ccc', 'a', 'bb', '', 'dddd']) as v));
select '-';
select toTypeName(dict), dict, lowCardinalityIndexes(dict), lowCardinalityKeys(dict) from (select '123_' || toLowCardinality(v) as dict from (select arrayJoin(['a', Null, 'bb', '', 'a', Null, 'ccc', 'a', 'bb', '', 'dddd']) as v));

select 'MergeTree';

drop table if exists test.lc_small_dict;
drop table if exists test.lc_big_dict;

create table test.lc_small_dict (str StringWithDictionary) engine = MergeTree order by str;
create table test.lc_big_dict (str StringWithDictionary) engine = MergeTree order by str;

insert into test.lc_small_dict select toString(number % 1000) from system.numbers limit 1000000;
insert into test.lc_big_dict select toString(number) from system.numbers limit 1000000;

detach table test.lc_small_dict;
detach table test.lc_big_dict;

attach table test.lc_small_dict;
attach table test.lc_big_dict;

select sum(toUInt64OrZero(str)) from test.lc_small_dict;
select sum(toUInt64OrZero(str)) from test.lc_big_dict;

drop table if exists test.lc_small_dict;
drop table if exists test.lc_big_dict;

