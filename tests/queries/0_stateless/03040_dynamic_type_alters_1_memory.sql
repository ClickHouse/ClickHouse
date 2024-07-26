set allow_experimental_dynamic_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;

drop table if exists test;
create table test (x UInt64, y UInt64) engine=Memory;
select 'initial insert';
insert into test select number, number from numbers(3);

select 'alter add column 1';
alter table test add column d Dynamic(max_types=3) settings mutations_sync=1;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.`Tuple(a UInt64)`.a from test order by x;

select 'insert after alter add column 1';
insert into test select number, number, number from numbers(3, 3);
insert into test select number, number, 'str_' || toString(number) from numbers(6, 3);
insert into test select number, number, NULL from numbers(9, 3);
insert into test select number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) from numbers(12, 3);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'alter modify column 1';
alter table test modify column d Dynamic(max_types=1) settings mutations_sync=1;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'insert after alter modify column 1';
insert into test select number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) from numbers(15, 4);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'alter modify column 2';
alter table test modify column d Dynamic(max_types=3) settings mutations_sync=1;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'insert after alter modify column 2';
insert into test select number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) from numbers(19, 4);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'alter modify column 3';
alter table test modify column y Dynamic settings mutations_sync=1;
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, y.UInt64, y.String, y.`Tuple(a UInt64)`.a, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

select 'insert after alter modify column 3';
insert into test select number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL), NULL from numbers(23, 3);
select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d);
select x, y, y.UInt64, y.String, y.`Tuple(a UInt64)`.a, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a from test order by x;

drop table test;