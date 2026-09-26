-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

-- Regression test: selectivity estimation must not cause undefined behavior
-- when estimateRanges() returns a value > 1.0 (e.g. many IN values with
-- over-counting statistics), producing a float-to-UInt64 overflow.
-- See STID 6239-5b30.
--
-- Statistics are only invoked when the WHERE clause contains multiple AND
-- conditions (optimizePrewhere.cpp only creates the estimator when
-- has_multiple_conditions is true). A single predicate does not trigger
-- the statistics code path.

SET allow_statistics = 1;
SET use_statistics = 1;

DROP TABLE IF EXISTS t_stats_overflow;
CREATE TABLE t_stats_overflow
(
    key UInt64,
    val UInt64 STATISTICS(countmin, tdigest)
)
ENGINE = MergeTree
ORDER BY key;

-- Insert few rows so row count is small relative to IN clause size
INSERT INTO t_stats_overflow SELECT number, number % 10 FROM numbers(100);
OPTIMIZE TABLE t_stats_overflow FINAL;

-- Compound WHERE (AND of two conditions) triggers the statistics estimator
-- during PREWHERE optimization. The IN clause with 200 values on a 100-row
-- table causes estimateRanges() to return selectivity > 1.0 because CountMin
-- returns non-zero estimates for each value and the sum exceeds total rows.
-- Before the fix, this caused UBSAN float-cast-overflow on the static_cast<UInt64>.
SELECT count() FROM t_stats_overflow WHERE val IN (
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
    50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
    60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
    70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
    80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
    90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
    130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
    140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
    150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169,
    170, 171, 172, 173, 174, 175, 176, 177, 178, 179,
    180, 181, 182, 183, 184, 185, 186, 187, 188, 189,
    190, 191, 192, 193, 194, 195, 196, 197, 198, 199
) AND key > 50;

-- Same test with fewer IN values but still enough to exceed selectivity 1.0
SELECT count() FROM t_stats_overflow WHERE val IN (
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
    50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
    60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
    70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
    80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
    90, 91, 92, 93, 94, 95, 96, 97, 98, 99
) AND key > 50;

DROP TABLE t_stats_overflow;
