set enable_dynamic_type = 1;
create table test (d Dynamic(max_types=3)) engine=Memory;
insert into test values (1::UInt8), (2::UInt16), (3::UInt32), (4::UInt64), ('5'::String), ('2020-01-01'::Date);
select toFloat64(d) from test;
select toUInt32(d) from test;
drop table test;
