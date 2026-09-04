-- Random settings limits: index_granularity=(None, 60000)
-- Tags: long

-- test is derived from 03246_alter_from_string_to_json.sql

set max_block_size = 20000;

drop table if exists test;

create table test (x UInt64, json String) engine=MergeTree order by x;

insert into test select number, toJSONString(map('key' || multiIf(number < 60000, number % 2, number < 120000, number % 2 + 2, number % 2 + 4), 'value' || number)) from numbers(200000);

alter table test modify column json JSON settings mutations_sync=1;

set optimize_read_in_order=0; -- disabling read in order optimization leads to error
select json from test order by x limit 10 offset 120000;
select json.key0, json.key1, json.key2, json.key3, json.key4, json.key5 from test order by x limit 10 offset 120000;

drop table test;
