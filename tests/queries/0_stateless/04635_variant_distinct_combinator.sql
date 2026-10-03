-- Regression for the distinctness semantics of the -Distinct combinator over Variant.
-- Two Variant values with different alternative types are distinct even when the underlying values compare
-- equal after a cast to the common supertype: 1::UInt8 and 1::UInt64 hash and compare as different Variant
-- values, so uniq / uniqExact (which support Variant natively) count 2. Routing sumDistinct / avgDistinct
-- through AggregateFunctionVariantAdapter would cast the argument to Nullable(UInt64) before -Distinct
-- deduplicates, collapsing [1::UInt8, 1::UInt64] to a single 1 and silently changing the result. So the
-- -Distinct combinator marks the combined function as distinctness-sensitive
-- (IAggregateFunctionCombinator::isDistinctnessSensitive, propagated by
-- AggregateFunctionFactory::tryGetProperties), which keeps it out of the adapter: a Variant argument reports
-- the original error, unchanged from before the adapter existed.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

CREATE TABLE t_variant_distinct (v Variant(UInt8, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_distinct VALUES (1::UInt8), (1::UInt64), (1::UInt64);

-- The same column has more than one distinct value under Variant equality.
SELECT 'uniq', uniq(v), uniqExact(v) FROM t_variant_distinct;

-- -Distinct combined functions must not silently disagree with that: they keep rejecting Variant.
SELECT sumDistinct(v) FROM t_variant_distinct; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avgDistinct(v) FROM t_variant_distinct; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The sensitivity is collected from all stripped combinator suffixes, so stacked forms are excluded too.
SELECT sumDistinctIf(v, 1) FROM t_variant_distinct; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumDistinctState(v) FROM t_variant_distinct; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumIfDistinct(v, 1) FROM t_variant_distinct; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Aggregates that support Variant natively still work with -Distinct: they see the genuine Variant values.
SELECT 'countDistinct', countDistinct(v) FROM t_variant_distinct;

-- Without a Variant argument -Distinct is unaffected.
SELECT 'sumDistinct plain', sumDistinct(x) FROM (SELECT arrayJoin([1, 1, 2]) AS x);

DROP TABLE t_variant_distinct;
