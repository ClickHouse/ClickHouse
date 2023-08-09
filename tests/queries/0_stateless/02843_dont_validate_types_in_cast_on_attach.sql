set allow_experimental_object_type=1;
drop table if exists test_json;
create table test_json (x UInt64, json JSON default CAST('{}', 'JSON')) engine=MergeTree order by x;
detach table test_json;
set allow_experimental_object_type=1;
attach table test_json;
drop table test_json;

