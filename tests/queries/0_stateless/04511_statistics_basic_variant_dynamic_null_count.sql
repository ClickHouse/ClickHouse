-- Regression test: `basic` statistics must expose `null_count` (and `estimateIsNull`) for
-- `Variant` and `Dynamic` columns, whose type default is `NULL` even without a `Nullable`
-- wrapper.  Before the fix, `StatisticsBasic::is_nullable` was set only via
-- `isNullableOrLowCardinalityNullable`, which missed `Variant` and `Dynamic`.  As a result
-- `hasNullCount()` returned false, `estimates.null_count` stayed NULL in
-- `system.parts_columns`, and `estimateIsNull` fell back to the generic selectivity factor
-- instead of using the exact count.

DROP TABLE IF EXISTS t_stats_variant_dynamic_null_count;

CREATE TABLE t_stats_variant_dynamic_null_count
(
    id UInt32,
    v  Variant(UInt32, String) STATISTICS(basic),
    d  Dynamic                 STATISTICS(basic)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SET materialize_statistics_on_insert = 1;

-- 10 rows: 3 NULLs, 7 non-NULLs in both columns.
INSERT INTO t_stats_variant_dynamic_null_count VALUES
    (0, NULL, NULL),
    (1, NULL, NULL),
    (2, NULL, NULL),
    (3, 3,    3),
    (4, 4,    4),
    (5, 5,    5),
    (6, 6,    6),
    (7, 7,    7),
    (8, 8,    8),
    (9, 9,    9);

-- Both columns must report null_count = 3 and default_count = 3.
-- Before the fix null_count was NULL for both.
SELECT column, estimates.null_count, estimates.default_count
FROM system.parts_columns
WHERE table = 't_stats_variant_dynamic_null_count' AND active AND database = currentDatabase()
  AND column IN ('d', 'v')
ORDER BY column;

DROP TABLE t_stats_variant_dynamic_null_count;
