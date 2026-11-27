SET allow_experimental_time_series_aggregate_functions = 1;

-- The values go through a table rather than array literals: constants that print identically (all NaNs print as `nan`)
-- are conflated by the analyzer, which would silently feed every aggregate the same array.
DROP TABLE IF EXISTS t_changes_nan_bits;
CREATE TABLE t_changes_nan_bits (id String, ts UInt32, v64 UInt64, v32 UInt32) ENGINE = Memory;

-- Two quiet NaNs with the same bit pattern, two quiet NaNs differing in the lowest payload bit, and +0 vs -0.
INSERT INTO t_changes_nan_bits VALUES
    ('1_same_nan', 89, 9221120237041090560, 2143289344), ('1_same_nan', 101, 9221120237041090560, 2143289344),
    ('2_different_nan', 89, 9221120237041090560, 2143289344), ('2_different_nan', 101, 9221120237041090561, 2143289345),
    ('3_signed_zero', 89, 0, 0), ('3_signed_zero', 101, 9223372036854775808, 2147483648);

-- Identical NaN bit patterns are treated as the same value, while different NaN bit patterns are changes.
SELECT
    id,
    timeSeriesChangesToGrid(120, 120, 0, 40)(ts, reinterpretAsFloat64(reinterpretAsFixedString(v64))) AS float64,
    timeSeriesChangesToGrid(120, 120, 0, 40)(ts, reinterpretAsFloat32(reinterpretAsFixedString(v32))) AS float32
FROM t_changes_nan_bits
GROUP BY id
ORDER BY id;

-- The same rule applies when the transition is merged across bucket summaries.
SELECT
    id,
    timeSeriesChangesToGrid(100, 120, 10, 40)(ts, reinterpretAsFloat64(reinterpretAsFixedString(v64))) AS float64,
    timeSeriesChangesToGrid(100, 120, 10, 40)(ts, reinterpretAsFloat32(reinterpretAsFixedString(v32))) AS float32
FROM t_changes_nan_bits
GROUP BY id
ORDER BY id;

DROP TABLE t_changes_nan_bits;
