SET allow_suspicious_low_cardinality_types = 1;

SELECT
    number,
    variantType(v),
    variantElement(v, 'LowCardinality(UInt64)') AS x,
    isNull(x) AS is_null
FROM
(
    SELECT
        number,
        CAST(toLowCardinality(toUInt64(number)), 'Variant(LowCardinality(UInt64), String)') AS v
    FROM numbers(3)
);
