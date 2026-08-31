-- test for https://github.com/ClickHouse/ClickHouse/issues/101265
-- `ToDateMonotonicity` used `DATE_LUT_MAX_DAY_NUM` (65535) as the boundary for every
-- template instantiation. For `toDate32` the correct boundary is `DATE_LUT_MAX_EXTEND_DAY_NUM` (120530)

DROP TABLE IF EXISTS t_todate32_mono;

CREATE TABLE t_todate32_mono (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_todate32_mono SELECT number + 100000 FROM numbers(50000);

-- Range [100000, 149999] straddles the Date32 boundary (120530). Only x=100000 matches.
SELECT count() FROM t_todate32_mono WHERE toDate32(x) = toDate32(100000);

-- Lookup with a constant on the day-number side of the boundary, value present in the range.
SELECT count() FROM t_todate32_mono WHERE toDate32(x) = toDate32(120000);

-- Lookup with a constant on the timestamp side of the Date32 boundary.
-- toDate32(130000) interprets 130000 as a Unix timestamp (1970-01-02 12:06:40 UTC),
-- so only rows whose x falls on the same UTC day [86400, 172799] match.
-- Of the inserted x in [100000, 149999], those with x >= 120530 are interpreted as
-- timestamps (all within the day), giving 149999 - 120530 + 1 = 29470 rows.
SELECT count() FROM t_todate32_mono WHERE toDate32(x) = toDate32(130000);

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    x UInt64
)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1;

INSERT INTO test VALUES (120529), (120530);

SELECT count()
FROM test
WHERE toDate32(x) = toDate32(120530)
SETTINGS use_primary_key = 1;

SELECT count()
FROM test
WHERE toDate32(x) = toDate32(120530)
SETTINGS use_primary_key = 0;
