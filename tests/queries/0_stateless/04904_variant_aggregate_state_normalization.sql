-- `AggregateFunctionVariantNull` does not alter state bytes, so it must preserve the
-- normalization performed by the nested `-If` combinator. This allows byte-compatible
-- states made from a native Variant argument to be combined by UNION ALL.
SELECT anyMerge(state)
FROM
(
    SELECT anyState(CAST(1, 'Variant(UInt64)')) AS state
    UNION ALL
    SELECT anyIfState(CAST(2, 'Variant(UInt64)'), 1) AS state
);
