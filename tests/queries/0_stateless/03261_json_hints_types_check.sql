set enable_json_type=1;
set allow_experimental_variant_type=0;
set allow_experimental_object_type=0;

select '{}'::JSON(a LowCardinality(Int128)); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select '{}'::JSON(a FixedString(100000)); -- {serverError ILLEGAL_COLUMN}
select '{}'::JSON(a Variant(Int32)); -- {serverError ILLEGAL_COLUMN}
select '{}'::JSON(a Object('json')); -- {serverError ILLEGAL_COLUMN}
