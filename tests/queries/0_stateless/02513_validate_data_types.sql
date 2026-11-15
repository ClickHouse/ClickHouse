-- Tags: no-fasttest

set allow_suspicious_low_cardinality_types=0;
select CAST(1000000, 'LowCardinality(UInt64)'); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
desc file(nonexist.json, JSONEachRow, 'lc LowCardinality(UInt64)'); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}

set allow_suspicious_fixed_string_types=0;
select CAST('', 'FixedString(1000)'); -- {serverError ILLEGAL_COLUMN}
desc file(nonexist.json, JSONEachRow, 'fs FixedString(1000)'); -- {serverError ILLEGAL_COLUMN}
