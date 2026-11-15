set allow_experimental_variant_type=1;

select NULL::Variant(String, UInt64);
select 42::UInt64::Variant(String, UInt64);
select 42::UInt32::Variant(String, UInt64); -- {serverError CANNOT_CONVERT_TYPE}
select now()::Variant(String, UInt64); -- {serverError CANNOT_CONVERT_TYPE}
select CAST(number % 2 ? NULL : number, 'Variant(String, UInt64)') from numbers(4);
select 'Hello'::LowCardinality(String)::Variant(LowCardinality(String), UInt64);
select 'Hello'::LowCardinality(Nullable(String))::Variant(LowCardinality(String), UInt64);
select 'NULL'::LowCardinality(Nullable(String))::Variant(LowCardinality(String), UInt64);
select 'Hello'::LowCardinality(Nullable(String))::Variant(LowCardinality(String), UInt64);
select CAST(CAST(number % 2 ? NULL : 'Hello', 'LowCardinality(Nullable(String))'), 'Variant(LowCardinality(String), UInt64)') from numbers(4);

select NULL::Variant(String, UInt64)::UInt64;
select NULL::Variant(String, UInt64)::Nullable(UInt64);
select '42'::Variant(String, UInt64)::UInt64;
select 'str'::Variant(String, UInt64)::UInt64; -- {serverError CANNOT_PARSE_TEXT}
select CAST(multiIf(number % 3 == 0, NULL::Variant(String, UInt64), number % 3 == 1, 'Hello'::Variant(String, UInt64), number::Variant(String, UInt64)), 'Nullable(String)') from numbers(6);
select CAST(multiIf(number == 1, NULL::Variant(String, UInt64), number == 2, 'Hello'::Variant(String, UInt64), number::Variant(String, UInt64)), 'UInt64') from numbers(6); -- {serverError CANNOT_PARSE_TEXT}


select number::Variant(UInt64)::Variant(String, UInt64)::Variant(Array(String), String, UInt64) from numbers(2);
select 'str'::Variant(String, UInt64)::Variant(String, Array(UInt64)); -- {serverError CANNOT_CONVERT_TYPE}
