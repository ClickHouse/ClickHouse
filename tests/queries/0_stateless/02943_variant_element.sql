set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;

select variantElement(NULL::Variant(String, UInt64), 'UInt64') from numbers(4);
select variantElement(number::Variant(String, UInt64), 'UInt64') from numbers(4);
select variantElement(number::Variant(String, UInt64), 'String') from numbers(4);
select variantElement((number % 2 ? NULL : number)::Variant(String, UInt64), 'UInt64') from numbers(4);
select variantElement((number % 2 ? NULL : number)::Variant(String, UInt64), 'String') from numbers(4);
select variantElement((number % 2 ? NULL : 'str_' || toString(number))::LowCardinality(Nullable(String))::Variant(LowCardinality(String), UInt64), 'LowCardinality(String)') from numbers(4);
select variantElement(NULL::LowCardinality(Nullable(String))::Variant(LowCardinality(String), UInt64), 'LowCardinality(String)') from numbers(4);
select variantElement((number % 2 ? NULL : number)::Variant(Array(UInt64), UInt64), 'Array(UInt64)') from numbers(4);
select variantElement(NULL::Variant(Array(UInt64), UInt64), 'Array(UInt64)') from numbers(4);
select variantElement(number % 2 ? NULL : range(number + 1), 'Array(UInt64)') from numbers(4);

select variantElement([[(number % 2 ? NULL : number)::Variant(String, UInt64)]], 'UInt64') from numbers(4);

