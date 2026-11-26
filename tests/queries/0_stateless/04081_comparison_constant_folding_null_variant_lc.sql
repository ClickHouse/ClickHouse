-- Tags: no-random-settings

-- Regression test for crash in getConstantResultForNonConstArguments when comparing
-- a LowCardinality column with a Variant NULL constant.
-- When use_variant_default_implementation_for_comparisons is disabled, the Variant
-- adaptor is bypassed and getConstantResultForNonConstArguments tries to insert NULL
-- into a non-nullable LowCardinality(UInt8) result column, causing:
-- "Logical error: ColumnUnique can't contain null values"
-- The fix skips constant folding when the result type cannot hold NULL.

SET allow_suspicious_variant_types = 1;
SET use_variant_default_implementation_for_comparisons = 0;

DROP TABLE IF EXISTS t_lc_variant_null;
CREATE TABLE t_lc_variant_null (x LowCardinality(String)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_lc_variant_null VALUES ('a'), ('b'), ('c');

-- These should not crash. Without the Variant adaptor, the comparison types are
-- incompatible (String vs Variant), so a proper error is expected — not a crash.
SELECT greaterOrEquals(CAST(NULL, 'Variant(Date, Date32)'), x) FROM t_lc_variant_null; -- { serverError NO_COMMON_TYPE }
SELECT greater(CAST(NULL, 'Variant(UInt32, Int64)'), x) FROM t_lc_variant_null; -- { serverError NO_COMMON_TYPE }
SELECT equals(x, CAST(NULL, 'Variant(UInt32, Int64)')) FROM t_lc_variant_null; -- { serverError NO_COMMON_TYPE }

-- Verify normal path (Variant adaptor enabled) still works correctly — returns NULL
SET use_variant_default_implementation_for_comparisons = 1;
SELECT greaterOrEquals(CAST(NULL, 'Variant(Date, Date32)'), x) FROM t_lc_variant_null;

DROP TABLE t_lc_variant_null;
