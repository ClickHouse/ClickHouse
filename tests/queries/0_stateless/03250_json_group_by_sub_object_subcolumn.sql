set use_variant_as_common_type = 1;

drop table if exists test;
create table test (json JSON(max_dynamic_paths = 20, `a.b.c` UInt32)) engine = Memory;
insert into test select toJSONString(map('a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(2);
select json.^a from test group by json.^a order by toString(json.^a);
drop table test;
