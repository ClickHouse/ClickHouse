-- Regression for groupArrayInsertAt / groupArraySorted over Variant. Their Field-based state cannot represent
-- a Variant value faithfully: the original alternative type is lost on ingest and the first compatible one is
-- reinferred on output (1::UInt8 and 1::UInt64 collapse), and groupArraySorted would order by Field comparison
-- instead of Variant order. So they do not claim native Variant support: a Variant argument goes through
-- AggregateFunctionVariantAdapter over the least common supertype of the variants, like sum / avg, and is
-- rejected with a clean error at resolution when there is no lossless supertype.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

CREATE TABLE t_variant_ga (pos UInt64, v Variant(UInt8, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_ga VALUES (0, 200::UInt8), (1, 100::UInt64), (2, 1::UInt8), (3, 1::UInt64);

-- Lossless supertype: adapted to Nullable(UInt64); no value is collapsed or reinferred, and the order is numeric.
SELECT groupArrayInsertAt(v, pos), toTypeName(groupArrayInsertAt(v, pos)) FROM t_variant_ga;
SELECT groupArraySorted(10)(v), toTypeName(groupArraySorted(10)(v)) FROM t_variant_ga;

-- No lossless supertype: rejected at resolution instead of returning silently wrong results.
SELECT groupArrayInsertAt(v, pos) FROM (SELECT 0::UInt64 AS pos, 'a'::Variant(String, UInt64) AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupArraySorted(10)(v) FROM (SELECT 'a'::Variant(String, UInt64) AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Dynamic is rejected as well.
SET allow_experimental_dynamic_type = 1;
SELECT groupArrayInsertAt(number::Dynamic, number) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupArraySorted(10)(number::Dynamic) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_variant_ga;
