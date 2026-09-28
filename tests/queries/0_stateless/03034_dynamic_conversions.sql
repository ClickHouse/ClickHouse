set allow_experimental_dynamic_type=1;
set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;

select number::Dynamic as d, dynamicType(d) from numbers(3);
select number::Dynamic(max_types=0) as d, dynamicType(d) from numbers(3);
select number::Dynamic::UInt64 as v from numbers(3);
select number::Dynamic::String as v from numbers(3);
select number::Dynamic::Date as v from numbers(3);
select number::Dynamic::Array(UInt64) as v from numbers(3); -- {serverError TYPE_MISMATCH}
select number::Dynamic::Variant(UInt64, String) as v, variantType(v) from numbers(3);
select (number % 2 ? NULL : number)::Dynamic as d, dynamicType(d) from numbers(3);

select multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, range(number), NULL)::Dynamic as d, dynamicType(d) from numbers(6);
select multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, range(number), NULL)::Dynamic(max_types=0) as d, dynamicType(d) from numbers(6);
select multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, range(number), NULL)::Dynamic(max_types=1) as d, dynamicType(d) from numbers(6);
select multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, range(number), NULL)::Dynamic(max_types=2) as d, dynamicType(d) from numbers(6);

select number::Dynamic(max_types=2)::Dynamic(max_types=3) as d from numbers(3);
select number::Dynamic(max_types=2)::Dynamic(max_types=1) as d from numbers(3);
select multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, range(number), NULL)::Dynamic(max_types=2)::Dynamic(max_types=1) as d, dynamicType(d) from numbers(6);
select multiIf(number % 4 == 0, number, number % 4 == 1, toDate(number), number % 4 == 2, range(number), NULL)::Dynamic(max_types=4)::Dynamic(max_types=3) as d, dynamicType(d) from numbers(6);


create table test (d Dynamic) engine = Memory;
insert into test values (NULL), (42), ('42.42'), (true), ('e10');
select d::Float64 from test;
select d::Nullable(Float64) from test;
select d::String from test;
select d::Nullable(String) from test;
select d::UInt64 from test; -- {serverError CANNOT_PARSE_TEXT}
select d::Nullable(UInt64) from test;
select d::Date from test; -- {serverError CANNOT_PARSE_DATE}

