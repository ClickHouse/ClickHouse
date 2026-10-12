-- Regression for the uniqueness semantics of singleValueOrNull over Variant.
-- Two Variant values with different alternative types are distinct even when the underlying values compare
-- equal after a cast to the common supertype: 1::UInt8 and 1::UInt64 hash and compare as different Variant
-- values, so uniq / uniqExact (which support Variant natively) count 2. Routing singleValueOrNull through
-- AggregateFunctionVariantAdapter would collapse both to 1::Nullable(UInt64) and return 1 where the contract
-- ("the value if there is exactly one distinct non-NULL value, otherwise NULL") requires NULL, contradicting
-- uniq over the same column. So singleValueOrNull is excluded from the adapter
-- (AggregateFunctionProperties::is_distinctness_sensitive) and keeps its original error, unchanged from before
-- the adapter existed.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

CREATE TABLE t_variant_svon (v Variant(UInt8, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_svon VALUES (1::UInt8), (1::UInt64);

-- The same column has more than one distinct value under Variant equality.
SELECT 'uniq', uniq(v), uniqExact(v) FROM t_variant_svon;

-- singleValueOrNull must not silently disagree with that: it keeps rejecting Variant.
SELECT singleValueOrNull(v) FROM t_variant_svon; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The exclusion is per base function, so combinator forms are excluded too.
SELECT singleValueOrNullIf(v, 1) FROM t_variant_svon; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT singleValueOrNullState(v) FROM t_variant_svon; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The `= ALL (SELECT ...)` operator is rewritten through singleValueOrNull, so it reports the same error
-- instead of returning a wrong answer for a subquery with distinct Variant values that cast to equal numbers.
SELECT 1 = ALL (SELECT v FROM t_variant_svon); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_variant_svon;
