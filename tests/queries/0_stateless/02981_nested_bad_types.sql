set allow_suspicious_low_cardinality_types=0;
set allow_suspicious_fixed_string_types=0;
set allow_experimental_variant_type=0;

select [42]::Array(LowCardinality(UInt64)); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select [[[42]]]::Array(Array(Array(LowCardinality(UInt64)))); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select map('a', 42)::Map(String, LowCardinality(UInt64)); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select map('a', map('b', [42]))::Map(String, Map(String, Array(LowCardinality(UInt64)))); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select tuple('a', 42)::Tuple(String, LowCardinality(UInt64)); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select tuple('a', [map('b', 42)])::Tuple(String, Array(Map(String, LowCardinality(UInt64)))); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}

select [42]::Array(FixedString(1000000)); -- {serverError ILLEGAL_COLUMN}
select [42]::Array(FixedString(1000000)); -- {serverError ILLEGAL_COLUMN}
select [[[42]]]::Array(Array(Array(FixedString(1000000)))); -- {serverError ILLEGAL_COLUMN}
select map('a', 42)::Map(String, FixedString(1000000)); -- {serverError ILLEGAL_COLUMN}
select map('a', map('b', [42]))::Map(String, Map(String, Array(FixedString(1000000)))); -- {serverError ILLEGAL_COLUMN}
select tuple('a', 42)::Tuple(String, FixedString(1000000)); -- {serverError ILLEGAL_COLUMN}
select tuple('a', [map('b', 42)])::Tuple(String, Array(Map(String, FixedString(1000000)))); -- {serverError ILLEGAL_COLUMN}

select [42]::Array(Variant(String, UInt64)); -- {serverError ILLEGAL_COLUMN}
select [42]::Array(Variant(String, UInt64)); -- {serverError ILLEGAL_COLUMN}
select [[[42]]]::Array(Array(Array(Variant(String, UInt64)))); -- {serverError ILLEGAL_COLUMN}
select map('a', 42)::Map(String, Variant(String, UInt64)); -- {serverError ILLEGAL_COLUMN}
select map('a', map('b', [42]))::Map(String, Map(String, Array(Variant(String, UInt64)))); -- {serverError ILLEGAL_COLUMN}
select tuple('a', 42)::Tuple(String, Variant(String, UInt64)); -- {serverError ILLEGAL_COLUMN}
select tuple('a', [map('b', 42)])::Tuple(String, Array(Map(String, Variant(String, UInt64)))); -- {serverError ILLEGAL_COLUMN}

