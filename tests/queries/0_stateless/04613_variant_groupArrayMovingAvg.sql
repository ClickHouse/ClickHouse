-- groupArrayMovingAvg promotes non-Decimal input to a Float64 accumulator (its result is Array(Float64)), so -- like
-- plain avg -- a numeric mix with no lossless common supertype (e.g. Int64 + Float64) is aggregated via the Float64
-- fallback of the Variant adapter. groupArrayMovingSum keeps the input's arithmetic type and is intentionally NOT
-- float-promoting, so the same mix keeps the original ILLEGAL_TYPE_OF_ARGUMENT error, exactly as before.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_gam_variant;
CREATE TABLE t_gam_variant (id UInt64, v Variant(Int64, Float64)) ENGINE = Memory;
INSERT INTO t_gam_variant VALUES (1, 1::Int64), (2, 2.5::Float64), (3, 4::Int64), (4, 7.5::Float64);

-- groupArrayMovingAvg over a numeric-mix Variant now succeeds and returns Array(Float64), matching the value it would
-- compute over the same input cast to Nullable(Float64).
SELECT 'type', toTypeName(groupArrayMovingAvg(v)) FROM (SELECT v FROM t_gam_variant ORDER BY id);
SELECT 'avg == Float64',
    groupArrayMovingAvg(v) = groupArrayMovingAvg(CAST(v AS Nullable(Float64)))
    FROM (SELECT v FROM t_gam_variant ORDER BY id);

-- The window-size parameter form goes through the same adapter.
SELECT 'avg(2) == Float64',
    groupArrayMovingAvg(2)(v) = groupArrayMovingAvg(2)(CAST(v AS Nullable(Float64)))
    FROM (SELECT v FROM t_gam_variant ORDER BY id);

-- groupArrayMovingSum is intentionally NOT float-promoting: a numeric mix with no lossless common supertype keeps
-- the original error, exactly as plain min/max/argMin do.
SELECT groupArrayMovingSum(v) FROM t_gam_variant; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_gam_variant;

-- A Variant with a lossless common supertype (here UInt64) is adapted for both forms without the Float64 fallback.
DROP TABLE IF EXISTS t_gam_lossless;
CREATE TABLE t_gam_lossless (id UInt64, v Variant(UInt8, UInt64)) ENGINE = Memory;
INSERT INTO t_gam_lossless VALUES (1, 1::UInt8), (2, 2::UInt64), (3, 3::UInt8);
SELECT 'lossless avg == Float64',
    groupArrayMovingAvg(v) = groupArrayMovingAvg(CAST(v AS Nullable(Float64)))
    FROM (SELECT v FROM t_gam_lossless ORDER BY id);
SELECT 'lossless sum type', toTypeName(groupArrayMovingSum(v)) FROM (SELECT v FROM t_gam_lossless ORDER BY id);
SELECT 'lossless sum == UInt64',
    groupArrayMovingSum(v) = groupArrayMovingSum(CAST(v AS Nullable(UInt64)))
    FROM (SELECT v FROM t_gam_lossless ORDER BY id);

DROP TABLE t_gam_lossless;
