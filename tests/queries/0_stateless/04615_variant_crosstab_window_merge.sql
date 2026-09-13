-- Regression for the Variant adapter forwarding cross-variant state merging (Window vs Aggregation) to the
-- nested CrossTab function. The CrossTab family (contingency / cramersV / cramersVBiasCorrected / theilsU) is
-- routed through the adapter for a Variant argument, so a window-produced state and an aggregation-produced
-- state of the same function must still merge, exactly as they do for a concrete (non-Variant) argument. This
-- needs the adapter to forward canMergeStateFromDifferentVariant / mergeStateFromDifferentVariant to the nested
-- function; without it the conversion below failed with CANNOT_CONVERT_TYPE. See 04204_crosstab_variant_merge
-- for the concrete-argument counterpart.

SET enable_analyzer = 1;
SET allow_suspicious_variant_types = 1;

-- Convert a window state (produced over a Variant argument, which the -State combinator normalizes to
-- Nullable(supertype)) to the declared Variant-form state type, which resolves to the adapter. The
-- AggregateFunction -> AggregateFunction conversion merges the differing state variant into a fresh target
-- through mergeStateFromDifferentVariant. The value must match the concrete-argument merge in 04204.
SELECT round(cramersVMerge(CAST(w AS AggregateFunction(cramersV, Variant(UInt8, UInt64), UInt8))), 4)
FROM (SELECT cramersVState(CAST(number % 10 AS Variant(UInt8, UInt64)), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(contingencyMerge(CAST(w AS AggregateFunction(contingency, Variant(UInt8, UInt64), UInt8))), 4)
FROM (SELECT contingencyState(CAST(number % 10 AS Variant(UInt8, UInt64)), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(cramersVBiasCorrectedMerge(CAST(w AS AggregateFunction(cramersVBiasCorrected, Variant(UInt8, UInt64), UInt8))), 4)
FROM (SELECT cramersVBiasCorrectedState(CAST(number % 10 AS Variant(UInt8, UInt64)), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(theilsUMerge(CAST(w AS AggregateFunction(theilsU, Variant(UInt8, UInt64), UInt8))), 4)
FROM (SELECT theilsUState(CAST(number % 10 AS Variant(UInt8, UInt64)), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

-- A declared Variant-form state column holding both an aggregation-produced and a window-produced state: the
-- window state is converted on INSERT and both are merged together by cramersVMerge, matching the value above.
DROP TABLE IF EXISTS t_04615;
CREATE TABLE t_04615 (s AggregateFunction(cramersV, Variant(UInt8, UInt64), UInt8)) ENGINE = Memory;
INSERT INTO t_04615 SELECT cramersVState(CAST(number % 10 AS Variant(UInt8, UInt64)), toUInt8(number % 6)) FROM numbers(100);
INSERT INTO t_04615 SELECT CAST(w AS AggregateFunction(cramersV, Variant(UInt8, UInt64), UInt8)) FROM (SELECT cramersVState(CAST(number % 10 AS Variant(UInt8, UInt64)), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);
SELECT round(cramersVMerge(s), 4) FROM t_04615;
DROP TABLE t_04615;
