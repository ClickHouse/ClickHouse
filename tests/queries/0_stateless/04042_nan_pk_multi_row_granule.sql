-- Tags: no-random-merge-tree-settings
-- PR #98964 fixed NaN handling in `checkInHyperrectangle` for index range
-- analysis. The PR's own test (`04031`) uses `index_granularity = 1` which
-- puts each value in its own granule, so the `right` bound of every granule
-- is the NaN value itself only for the last granule (left.isNaN() path).
-- With `index_granularity = 8192` all values fall in one granule whose right
-- bound is NaN, exercising the `key_range.right.isNaN()` branch at
-- `KeyCondition.cpp:4205`.

DROP TABLE IF EXISTS t_04042_nan;

CREATE TABLE t_04042_nan (col Float32)
ENGINE = MergeTree ORDER BY col
SETTINGS index_granularity = 8192;

-- Sorted order: -inf, -1, 0, 1, 2, 3, inf, nan
INSERT INTO t_04042_nan SELECT arrayJoin([-inf, -1, 0, 1, 2, 3, inf, nan])::Float32;

-- Granule spans [-inf, nan]; right bound is NaN — exercises the else-if branch
SELECT count() FROM t_04042_nan WHERE col > 0;
SELECT count() FROM t_04042_nan WHERE col >= 0;
SELECT count() FROM t_04042_nan WHERE col < 0;
SELECT count() FROM t_04042_nan WHERE col <= 0;
SELECT count() FROM t_04042_nan WHERE col = 0;
SELECT count() FROM t_04042_nan WHERE col BETWEEN -1 AND 3;

DROP TABLE t_04042_nan;

-- Nullable(Float32) PK: NaN and NULL in the same granule
DROP TABLE IF EXISTS t_04042_nan_n;

CREATE TABLE t_04042_nan_n (col Nullable(Float32))
ENGINE = MergeTree ORDER BY col
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

INSERT INTO t_04042_nan_n SELECT arrayJoin([NULL, -1, 0, 1, 2, nan, NULL])::Nullable(Float32);

SELECT count() FROM t_04042_nan_n WHERE col > 0;
SELECT count() FROM t_04042_nan_n WHERE col < 0;
SELECT count() FROM t_04042_nan_n WHERE col = 0;

DROP TABLE t_04042_nan_n;
