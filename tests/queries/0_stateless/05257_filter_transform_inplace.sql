-- 03999_filter_transform_inplace.sql
-- Exercises in-place column filtering in FilterTransform (the plain WHERE path).
-- optimize_move_to_prewhere = 0 keeps conditions in WHERE so FilterTransform runs
-- instead of MergeTreeRangeReader (PREWHERE). Results must be unaffected by the
-- in-place optimization, this test guards that.

SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_inplace;
CREATE TABLE t_inplace
(
    id       UInt64,
    et       UInt8,
    s        String,
    fs       FixedString(6),
    arr      Array(UInt32),
    nul      Nullable(Int64),
    lc       LowCardinality(String),
    tup      Tuple(UInt32, String)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_inplace
SELECT
    number,
    number % 100,
    concat('s_', toString(number)),
    toFixedString(leftPad(toString(number % 1000), 6, '0'), 6),
    range(number % 5),
    if(number % 3 = 0, NULL, toInt64(number)),
    concat('c_', toString(number % 10)),
    tuple(toUInt32(number % 50), concat('t', toString(number % 3)))
FROM numbers(100000);

-- High selectivity (most rows survive): exercises the in-place compaction heavily.
SELECT count(), sum(id), sum(et) FROM t_inplace WHERE et < 90;

-- Low selectivity (few rows survive).
SELECT count(), sum(id) FROM t_inplace WHERE et = 42;

-- Composite / variable-length columns survive the filter.
SELECT count(), sum(length(s)), sum(length(arr)), sum(length(lc)), sum(tup.1)
FROM t_inplace WHERE et % 7 = 0;

-- FixedString and nullable (NULL must be treated as false).
SELECT count(), sum(ifNull(nul, 0)), min(fs), max(fs)
FROM t_inplace WHERE et % 2 = 0;

-- Filter expression is also selected (remove_filter_column = false):
-- the mask column must take the copy path, never in-place.
SELECT (et = 42) AS is42, count() FROM t_inplace WHERE et = 42 GROUP BY is42;

-- Nothing survives.
SELECT count() FROM t_inplace WHERE et = 200;

-- Everything survives.
SELECT count() FROM t_inplace WHERE et < 100;

DROP TABLE t_inplace;
