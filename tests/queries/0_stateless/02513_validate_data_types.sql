-- Tags: no-fasttest

set allow_experimental_object_type=0;
select CAST('{"x" : 1}', 'JSON'); -- {serverError ILLEGAL_COLUMN}
desc file(nonexist.json, JSONAsObject); -- {serverError ILLEGAL_COLUMN}
desc file(nonexist.json, JSONEachRow, 'x JSON'); -- {serverError ILLEGAL_COLUMN}

set allow_experimental_geo_types=0;
select CAST([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]], 'Polygon'); -- {serverError ILLEGAL_COLUMN}
desc file(nonexist.json, JSONEachRow, 'pg Polygon'); -- {serverError ILLEGAL_COLUMN}

set allow_suspicious_low_cardinality_types=0;
select CAST(1000000, 'LowCardinality(UInt64)'); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}
desc file(nonexist.json, JSONEachRow, 'lc LowCardinality(UInt64)'); -- {serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY}

set allow_suspicious_fixed_string_types=0;
select CAST('', 'FixedString(1000)'); -- {serverError ILLEGAL_COLUMN}
desc file(nonexist.json, JSONEachRow, 'fs FixedString(1000)'); -- {serverError ILLEGAL_COLUMN}

