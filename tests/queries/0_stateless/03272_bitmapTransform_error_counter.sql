-- Tags: no-parallel

CREATE TABLE counters (value UInt64) ENGINE = MergeTree() ORDER BY value;

INSERT INTO counters SELECT sum(value) FROM system.errors WHERE name = 'ILLEGAL_TYPE_OF_ARGUMENT';

SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res FORMAT Null;

INSERT INTO counters SELECT sum(value) FROM system.errors WHERE name = 'ILLEGAL_TYPE_OF_ARGUMENT';

SELECT (max(value) - min(value)) == 0 FROM counters;
