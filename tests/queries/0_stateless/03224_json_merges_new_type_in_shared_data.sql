set allow_experimental_json_type = 1;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=1)) engine=MergeTree order by tuple() settings min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
insert into test select '{"b" : 42}' from numbers(5);
insert into test select '{"a" : 42, "b" : [1, 2, 3]}' from numbers(5);
optimize table test final;
select distinct dynamicType(json.b) as type, isDynamicElementInSharedData(json.b) from test order by type;
insert into test select '{"b" : 42}' from numbers(5);
optimize table test final;
select distinct dynamicType(json.b) as type, isDynamicElementInSharedData(json.b) from test order by type;
drop table test;
