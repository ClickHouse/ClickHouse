set allow_experimental_dynamic_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;

drop table if exists test;
create table test (x UInt64, y UInt64) engine=MergeTree order by x settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;

select 'initial insert';
insert into test select number, number from numbers(3);

select 'alter add column';
alter table test add column d Dynamic settings mutations_sync=1;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.`Tuple(a UInt64)`.a from test order by x;

select 'insert after alter add column 1';
insert into test select number, number, number from numbers(3, 3);
insert into test select number, number, 'str_' || toString(number) from numbers(6, 3);
insert into test select number, number, NULL from numbers(9, 3);
insert into test select number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) from numbers(12, 3);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'alter rename column 1';
alter table test rename column d to d1 settings mutations_sync=1;
select count(), dynamicType(d1) from test group by dynamicType(d1) order by count(), dynamicType(d1);
select x, y, d1, d1.String, d1.UInt64, d1.Date, d1.`Tuple(a UInt64)`.a from test order by x;

select 'insert nested dynamic';
insert into test select number, number, [number % 2 ? number : 'str_' || toString(number)]::Array(Dynamic) from numbers(15, 3);
select count(), dynamicType(d1) from test group by dynamicType(d1) order by count(), dynamicType(d1);
select x, y, d1, d1.String, d1.UInt64, d1.Date, d1.`Tuple(a UInt64)`.a, d1.`Array(Dynamic)`.UInt64, d1.`Array(Dynamic)`.String, d1.`Array(Dynamic)`.Date from test order by x;

select 'alter rename column 2';
alter table test rename column d1 to d2 settings mutations_sync=1;
select count(), dynamicType(d2) from test group by dynamicType(d2) order by count(), dynamicType(d2);
select x, y, d2, d2.String, d2.UInt64, d2.Date, d2.`Tuple(a UInt64)`.a, d2.`Array(Dynamic)`.UInt64, d2.`Array(Dynamic)`.String, d2.`Array(Dynamic)`.Date, from test order by x;

drop table test;
