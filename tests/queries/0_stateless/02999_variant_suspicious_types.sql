set allow_suspicious_variant_types=0;
select 42::Variant(UInt32, Int64); -- {serverError ILLEGAL_COLUMN}
select [42]::Variant(Array(UInt32), Array(Int64)); -- {serverError ILLEGAL_COLUMN}
select 'Hello'::Variant(String, LowCardinality(String)); -- {serverError ILLEGAL_COLUMN}
select (1, 'Hello')::Variant(Tuple(UInt32, String), Tuple(Int64, String)); -- {serverError ILLEGAL_COLUMN}
select map(42, 42)::Variant(Map(UInt64, UInt32), Map(UInt64, Int64)); -- {serverError ILLEGAL_COLUMN}

