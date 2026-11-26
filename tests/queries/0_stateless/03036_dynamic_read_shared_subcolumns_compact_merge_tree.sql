-- Tags: long, no-tsan, no-msan, no-ubsan, no-asan

set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set allow_experimental_dynamic_type = 1;

drop table if exists test;
create table test (id UInt64, d Dynamic(max_types=2)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;

insert into test select number, number from numbers(100000) settings min_insert_block_size_rows=50000;
insert into test select number, 'str_' || toString(number) from numbers(100000, 100000) settings min_insert_block_size_rows=50000;
insert into test select number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1)) from numbers(200000, 100000) settings min_insert_block_size_rows=50000;
insert into test select number, NULL from numbers(300000, 100000) settings min_insert_block_size_rows=50000;
insert into test select number, multiIf(number % 4 == 3, 'str_' || toString(number), number % 4 == 2, NULL, number % 4 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1))) from numbers(400000, 400000) settings min_insert_block_size_rows=50000;
insert into test select number, if (number % 5 == 1, [range((number % 10 + 1)::UInt64)]::Array(Array(Dynamic)), number) from numbers(100000, 100000) settings min_insert_block_size_rows=50000;
insert into test select number, if (number % 5 == 1, ('str_' || number)::LowCardinality(String)::Dynamic, number::Dynamic) from numbers(100000, 100000) settings min_insert_block_size_rows=50000;

select distinct dynamicType(d) as type from test order by type;
select count() from test where dynamicType(d) == 'UInt64';
select count() from test where d.UInt64 is not NULL;
select count() from test where dynamicType(d) == 'String';
select count() from test where d.String is not NULL;
select count() from test where dynamicType(d) == 'Date';
select count() from test where d.Date is not NULL;
select count() from test where dynamicType(d) == 'LowCardinality(String)';
select count() from test where d.`LowCardinality(String)` is not NULL;
select count() from test where dynamicType(d) == 'Array(Variant(String, UInt64))';
select count() from test where not empty(d.`Array(Variant(String, UInt64))`);
select count() from test where dynamicType(d) == 'Array(Array(Dynamic))';
select count() from test where not empty(d.`Array(Array(Dynamic))`);
select count() from test where d is NULL;
select count() from test where not empty(d.`Tuple(a Array(Dynamic))`.a.String);

select d, d.UInt64, d.String, d.`Array(Variant(String, UInt64))` from test format Null;
select d.UInt64, d.String, d.`Array(Variant(String, UInt64))` from test format Null;
select d.Int8, d.Date, d.`LowCardinality(String)`, d.`Array(String)` from test format Null;
select d, d.UInt64, d.Date, d.`LowCardinality(String)`, d.`Array(Variant(String, UInt64))`, d.`Array(Variant(String, UInt64))`.size0, d.`Array(Variant(String, UInt64))`.UInt64 from test format Null;
select d.UInt64, d.Date, d.`LowCardinality(String)`, d.`Array(Variant(String, UInt64))`, d.`Array(Variant(String, UInt64))`.size0, d.`Array(Variant(String, UInt64))`.UInt64, d.`Array(Variant(String, UInt64))`.String from test format Null;
select d, d.`Tuple(a UInt64, b String)`.a, d.`Array(Dynamic)`.`Variant(String, UInt64)`.UInt64, d.`Array(Variant(String, UInt64))`.UInt64 from test format Null;
select d.`Array(Dynamic)`.`Variant(String, UInt64)`.UInt64, d.`Array(Dynamic)`.size0, d.`Array(Variant(String, UInt64))`.UInt64 from test format Null;
select d.`Array(Array(Dynamic))`.size1, d.`Array(Array(Dynamic))`.UInt64, d.`Array(Array(Dynamic))`.`Map(String, Tuple(a UInt64))`.values.a from test format Null;
    
drop table test;
