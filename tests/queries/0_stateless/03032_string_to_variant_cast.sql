set allow_experimental_variant_type=1;
select CAST('42', 'Variant(String, UInt64)') as v, variantType(v);
select CAST('abc', 'Variant(String, UInt64)') as v, variantType(v);
select CAST('null', 'Variant(String, UInt64)') as v, variantType(v);
select CAST('[1, 2, 3]', 'Variant(String, Array(UInt64))') as v, variantType(v);
select CAST('[1, 2, 3', 'Variant(String, Array(UInt64))') as v, variantType(v);
select CAST('42', 'Variant(Date)') as v, variantType(v); -- {serverError INCORRECT_DATA}
select accurateCastOrNull('42', 'Variant(Date)') as v, variantType(v);

select CAST('42'::FixedString(2), 'Variant(String, UInt64)') as v, variantType(v);
select CAST('42'::LowCardinality(String), 'Variant(String, UInt64)') as v, variantType(v);
select CAST('42'::Nullable(String), 'Variant(String, UInt64)') as v, variantType(v);
select CAST(NULL::Nullable(String), 'Variant(String, UInt64)') as v, variantType(v);
select CAST('42'::LowCardinality(Nullable(String)), 'Variant(String, UInt64)') as v, variantType(v);
select CAST(NULL::LowCardinality(Nullable(String)), 'Variant(String, UInt64)') as v, variantType(v);
select CAST(NULL::LowCardinality(Nullable(FixedString(2))), 'Variant(String, UInt64)') as v, variantType(v);

