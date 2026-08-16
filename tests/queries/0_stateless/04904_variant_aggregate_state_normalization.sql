-- `AggregateFunctionVariantNull` does not alter state bytes, so it must preserve the
-- normalization performed by the nested `-If` combinator. This allows byte-compatible
-- states made from a native Variant argument to be combined by UNION ALL.
SELECT uniqExactMerge(state)
FROM
(
    SELECT uniqExactState(CAST(1::UInt64, 'Variant(UInt64)')) AS state
    UNION ALL
    SELECT uniqExactIfState(CAST(2::UInt64, 'Variant(UInt64)'), 1) AS state
);
