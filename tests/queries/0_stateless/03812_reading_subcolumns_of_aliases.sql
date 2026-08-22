set enable_analyzer=1;
set optimize_functions_to_subcolumns=1;

drop table if exists test;
create table test
(
    id UInt64,
    array Array(UInt32),
    tuple Tuple(array Array(UInt32)),
    json JSON,
    array_alias_1 Array(UInt32) alias array,
    array_alias_2 Array(String) alias array::Array(String),
    array_alias_3 Array(UInt32) alias tuple.array,
    array_alias_4 Array(String) alias tuple.array::Array(String),
    array_alias_5 Array(UInt32) alias json.array::Array(UInt32)
);

insert into test select 42, [1,2,3], tuple([1,2,3]), '{"array" : [1,2,3]}';
select id, array_alias_1.size0, array_alias_2.size0, array_alias_3.size0, array_alias_4.size0, array_alias_5.size0 from test;
select id, length(array_alias_1), length(array_alias_2), length(array_alias_3), length(array_alias_4), length(array_alias_5) from test;
drop table test;
