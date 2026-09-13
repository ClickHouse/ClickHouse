set allow_experimental_dynamic_type=1;
select max(number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select min(number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select argMax(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select argMin(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select anyArgMax(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select anyArgMin(number, number::Dynamic) from numbers(10); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
create table test (d Dynamic, index idx d type minmax); -- {serverError BAD_ARGUMENTS}

set allow_experimental_variant_type=1;
-- Aggregate functions that do not support Variant natively now aggregate over the least common supertype of the
-- variants, wrapped in Nullable (see AggregateFunctionVariantAdapter), instead of throwing ILLEGAL_TYPE_OF_ARGUMENT.
select max(number::Variant(UInt64)) from numbers(10);
select min(number::Variant(UInt64)) from numbers(10);
select argMax(number, number::Variant(UInt64)) from numbers(10);
select argMin(number, number::Variant(UInt64)) from numbers(10);
select anyArgMax(number, number::Variant(UInt64)) from numbers(10);
select anyArgMin(number, number::Variant(UInt64)) from numbers(10);
create table test (d Variant(UInt64), index idx d type minmax); -- {serverError BAD_ARGUMENTS}

