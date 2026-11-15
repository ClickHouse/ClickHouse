select 'NativeReader';
select toTypeName(dict), dict, lowCardinalityIndices(dict), lowCardinalityKeys(dict) from (select '123_' || toLowCardinality(v) as dict from (select arrayJoin(['a', 'bb', '', 'a', 'ccc', 'a', 'bb', '', 'dddd']) as v));
select '-';
select toTypeName(dict), dict, lowCardinalityIndices(dict), lowCardinalityKeys(dict) from (select '123_' || toLowCardinality(v) as dict from (select arrayJoin(['a', Null, 'bb', '', 'a', Null, 'ccc', 'a', 'bb', '', 'dddd']) as v));

select 'MergeTree';

drop table if exists lc_small_dict;
drop table if exists lc_big_dict;

create table lc_small_dict (str LowCardinality(String)) engine = MergeTree order by str SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
create table lc_big_dict (str LowCardinality(String)) engine = MergeTree order by str SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into lc_small_dict select toString(number % 1000) from system.numbers limit 1000000;
insert into lc_big_dict select toString(number) from system.numbers limit 1000000;

detach table lc_small_dict;
detach table lc_big_dict;

attach table lc_small_dict;
attach table lc_big_dict;

select sum(toUInt64OrZero(str)) from lc_small_dict;
select sum(toUInt64OrZero(str)) from lc_big_dict;

drop table if exists lc_small_dict;
drop table if exists lc_big_dict;
