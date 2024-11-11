select _CAST(1, 'Dynamic'); -- {serverError ILLEGAL_COLUMN}
select _CAST('{}', 'JSON'); -- {serverError ILLEGAL_COLUMN}
select _CAST('{}', 'Object(\'JSON\')'); -- {serverError ILLEGAL_COLUMN}
select _CAST('1', 'LowCardinality(Int128)'); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
select _CAST('abc', 'FixedString(10000000)'); -- {serverError ILLEGAL_COLUMN}

