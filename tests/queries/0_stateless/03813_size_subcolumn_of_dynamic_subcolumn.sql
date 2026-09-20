drop table if exists test;
create table test (ad Array(Array(Dynamic)), jd Array(Array(JSON))) engine=MergeTree order by tuple();
insert into test select [[[1, 2, 3]::Array(UInt64)::Dynamic]], [['{"a" : [1, 2, 3], "b" : [{"c" : [[1, 2, 3]]}]}']];
select ad.`Array(UInt64)`.size2, jd.a.:`Array(Nullable(Int64))`.size2, jd.b[].c.:`Array(Array(Nullable(Int64)))`.size3, jd.b[].c.:`Array(Array(Nullable(Int64)))`.size4 from test;
select ad.`Array(UInt64)`.size0, jd.a.:`Array(Nullable(Int64))`.size0, jd.b[].c.:`Array(Array(Nullable(Int64)))`.size0, jd.b[].c.:`Array(Array(Nullable(Int64)))`.size1 from test; -- {serverError UNKNOWN_IDENTIFIER}
drop table test;
