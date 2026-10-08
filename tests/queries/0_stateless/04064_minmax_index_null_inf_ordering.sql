-- Test that minmax skip index correctly handles Nullable columns,
-- verifying -Inf/NULL/+Inf ordering in range analysis.

DROP TABLE IF EXISTS t_minmax_null;

CREATE TABLE t_minmax_null
(
    id UInt64,
    val Nullable(Int64),
    INDEX idx_val val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

-- Insert data in carefully chosen groups to create separate granules.
-- Granule 1: only NULLs
INSERT INTO t_minmax_null VALUES (1, NULL), (2, NULL), (3, NULL);
-- Granule 2: mix of NULLs and values
INSERT INTO t_minmax_null VALUES (4, NULL), (5, 10), (6, 20);
-- Granule 3: only positive values
INSERT INTO t_minmax_null VALUES (7, 100), (8, 200), (9, 300);
-- Granule 4: negative values
INSERT INTO t_minmax_null VALUES (10, -50), (11, -10), (12, 0);

-- Force merge into a single part so all granules are in one part.
OPTIMIZE TABLE t_minmax_null FINAL;

-- Test 1: IS NULL should read granules containing NULLs (granules 1 and 2) but skip granule 3 and 4.
SELECT id FROM t_minmax_null WHERE val IS NULL ORDER BY id;

-- Test 2: Equality on a value present only in granule 3.
SELECT id FROM t_minmax_null WHERE val = 200 ORDER BY id;

-- Test 3: Range that spans negative values (granule 4 only for non-null matches).
SELECT id FROM t_minmax_null WHERE val >= -50 AND val <= 0 ORDER BY id;

-- Test 4: val > 0 should not return NULLs.
SELECT id FROM t_minmax_null WHERE val > 0 ORDER BY id;

-- Test 5: IS NOT NULL should return all non-null rows.
SELECT id FROM t_minmax_null WHERE val IS NOT NULL ORDER BY id;

-- Test 6: Float column with NaN to verify NaN ordering in comparisons.
DROP TABLE IF EXISTS t_minmax_float;

CREATE TABLE t_minmax_float
(
    id UInt64,
    val Nullable(Float64),
    INDEX idx_val val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO t_minmax_float VALUES (1, NULL), (2, NULL), (3, NULL);
INSERT INTO t_minmax_float VALUES (4, nan), (5, nan), (6, nan);
INSERT INTO t_minmax_float VALUES (7, 1.0), (8, 2.0), (9, 3.0);
INSERT INTO t_minmax_float VALUES (10, -1.0), (11, 0.0), (12, 1.0);
OPTIMIZE TABLE t_minmax_float FINAL;

-- NaN = NaN is false (IEEE 754), so this query returns no rows.
-- The skip index must not cause incorrect results here.
SELECT id FROM t_minmax_float WHERE val = nan ORDER BY id;

-- NULL rows
SELECT id FROM t_minmax_float WHERE val IS NULL ORDER BY id;

-- Normal range query should not include NaN or NULL rows.
SELECT id FROM t_minmax_float WHERE val >= 0.0 AND val <= 3.0 ORDER BY id;

DROP TABLE t_minmax_null;
DROP TABLE t_minmax_float;
