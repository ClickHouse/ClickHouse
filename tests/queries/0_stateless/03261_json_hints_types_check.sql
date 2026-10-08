select '{}'::JSON(a LowCardinality(Int128)); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select '{}'::JSON(a FixedString(100000)); -- {serverError ILLEGAL_COLUMN}
