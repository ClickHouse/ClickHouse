drop table if exists test;
create table test (`my.json` JSON) engine=Memory;
insert into test select '{"a" : 42}';
select my.json.a from test settings enable_analyzer=1;
select `my.json`.a from test settings enable_analyzer=1;
select my.json.a from test settings enable_analyzer=0;
select `my.json`.a from test settings enable_analyzer=0;
drop table test;

select `t.t`.a from format(JSONEachRow, '`t.t` Tuple(a UInt32)', '{"t.t" : {"a" : 42}}');

create table test
(
    `my.json` JSON(a UInt32),
    a1 UInt32 materialized my.json.a,
    a2 UInt32 default my.json.a,
    b1 UInt32 materialized my.json.b,
    b2 UInt32 default my.json.b,
    index idx1 my.json.a type minmax,
    index idx2 my.json.b::Int64 type minmax,
    projection prj1 (select my.json, my.json.a, my.json.b order by my.json.a, my.json.b::Int32)
) engine=MergeTree order by (my.json.a, my.json.b::Int32, my.json.a + 42, my.json.b::Int32 + 42);
insert into test (my.json) select '{"a" : 42, "b" : 42}';
select * from test;
select * from test order by my.json.a;
select * from test order by my.json.b::Int32;
insert into test (my.json) select '{"a" : 43, "b" : 43}';
optimize table test final;
select * from test;
select * from test order by my.json.a;
select * from test order by my.json.b::Int32;

alter table test modify column my.json JSON(a UInt32, b UInt32); -- {serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
alter table test update `my.json` = '{}' where 1; -- {serverError CANNOT_UPDATE_COLUMN}

drop table test;

create table test
(
    `my.tuple` Tuple(a UInt32),
    a1 UInt32 materialized my.tuple.a,
    a2 UInt32 default my.tuple.a,
    index idx1 my.tuple.a type minmax,
    projection prj1 (select my.tuple, my.tuple.a order by my.tuple.a)
) engine=MergeTree order by (my.tuple.a, my.tuple.a + 42);
insert into test (my.tuple) select tuple(42);
select * from test;
select * from test order by my.tuple.a;
insert into test (my.tuple) select tuple(43);
optimize table test final;
select * from test;
select * from test order by my.tuple.a;

alter table test modify column my.tuple Tuple(a UInt32, b UInt32); -- {serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
alter table test update `my.tuple` = tuple(0, 0) where 1; -- {serverError CANNOT_UPDATE_COLUMN}

drop table test;