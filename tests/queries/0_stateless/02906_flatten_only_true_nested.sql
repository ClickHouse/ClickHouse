set flatten_nested = 1;
drop table if exists test_nested;
create table test_nested (data Nested(x UInt32, y UInt32)) engine=Memory;
desc test_nested;
drop table test_nested;
drop table if exists test_array_tuple;
create table test_array_tuple (data Array(Tuple(x UInt64, y UInt64))) engine=Memory;
desc test_array_tuple;
drop table test_array_tuple;
