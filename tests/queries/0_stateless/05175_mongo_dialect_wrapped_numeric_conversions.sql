-- `Nullable` and `LowCardinality` do not change the value space of a type, so a wrapped numeric
-- column keeps the Mongo semantics of `$toDate` - a number is a count of Unix milliseconds - and
-- a wrapped integer or boolean is still an exact scale-0 `$toDecimal`. Before the fix both
-- lowerings keyed off the raw `toTypeName` text, so `Nullable(Int64)` read milliseconds as
-- seconds (the year 2282) and `Nullable(Int64)` was rejected by `$toDecimal` altogether.
SET dialect = 'clickhouse';
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS wrapped_numeric;
CREATE TABLE wrapped_numeric
(
    ms Int64,
    ms_nullable Nullable(Int64),
    ms_low_cardinality LowCardinality(Int64),
    flag_nullable Nullable(Bool)
) ENGINE = MergeTree ORDER BY ms;
INSERT INTO wrapped_numeric VALUES (1546300800000, 1546300800000, 1546300800000, true);

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';
db.wrapped_numeric.aggregate([{"$project" : {"plain" : {"$toDate" : "$ms"}, "nullable" : {"$toDate" : "$ms_nullable"}, "low_cardinality" : {"$toDate" : "$ms_low_cardinality"}}}]);
db.wrapped_numeric.aggregate([{"$project" : {"nullable" : {"$toDecimal" : "$ms_nullable"}, "low_cardinality" : {"$toDecimal" : "$ms_low_cardinality"}, "flag" : {"$toDecimal" : "$flag_nullable"}}}]);

SET dialect = 'clickhouse';
DROP TABLE wrapped_numeric;
