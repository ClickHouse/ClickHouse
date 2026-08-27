-- test for https://github.com/ClickHouse/ClickHouse/issues/101265
-- `ToDateMonotonicity` used `DATE_LUT_MAX_DAY_NUM` (65535) as the boundary for every
-- template instantiation. For `toDate32` the correct boundary is `DATE_LUT_MAX_EXTEND_DAY_NUM` (2932896)

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_todate32_mono;

CREATE TABLE t_todate32_mono (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_todate32_mono SELECT number + 2900000 FROM numbers(50000);

-- Range [2900000, 2949999] straddles the Date32 boundary (2932896). Only x=2900000 matches.
SELECT count() FROM t_todate32_mono WHERE toDate32(x) = toDate32(2900000);

-- Lookup with a constant on the day-number side of the boundary, value present in the range.
SELECT count() FROM t_todate32_mono WHERE toDate32(x) = toDate32(2932890);

-- Lookup with a constant on the timestamp side of the Date32 boundary.
-- toDate32(3000000) interprets 3000000 as a Unix timestamp (1970-02-04 17:20:00 UTC),
-- so only rows whose x falls on the same UTC day [2937600, 3023999] match.
-- Of the inserted x in [2900000, 2949999], those with x in [2937600, 2949999] match, giving 12400 rows.
SELECT count() FROM t_todate32_mono WHERE toDate32(x) = toDate32(3000000);

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    x UInt64
)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1;

INSERT INTO test VALUES (2932896), (2932897);

SELECT count()
FROM test
WHERE toDate32(x) = toDate32(2932896)
SETTINGS use_primary_key = 1;

SELECT count()
FROM test
WHERE toDate32(x) = toDate32(2932896)
SETTINGS use_primary_key = 0;
