-- Regression test for off-by-one in ToDateMonotonicity boundary check.
-- The toDate function treats values <= DATE_LUT_MAX_DAY_NUM (65535) as day numbers
-- and values > 65535 as unix timestamps. The monotonicity check must correctly
-- identify ranges crossing this boundary as non-monotonic.
-- Previously caused LOGICAL_ERROR "Invalid binary search result in MergeTreeSetIndex" in debug builds.
-- https://github.com/ClickHouse/ClickHouse/issues/90461

DROP TABLE IF EXISTS t_todate_mono;

CREATE TABLE t_todate_mono (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_todate_mono SELECT number FROM numbers(100000);

-- With index_granularity=1, mark 65535 covers the range [65535, 65536],
-- which crosses the DATE_LUT_MAX_DAY_NUM boundary.
-- The toDate conversion in the key condition chain must report this range as non-monotonic.
SELECT count() > 0 FROM t_todate_mono WHERE toDate(x) IN (toDate(12345), toDate(67890));

DROP TABLE t_todate_mono;
