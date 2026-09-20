SELECT
    'empty',
    uniqHLL12(number),
    uniqCombined(12)(number),
    uniqCombined(13)(number),
    uniqCombined(20)(number),
    uniqCombined64(12)(number),
    uniqCombined64(13)(number),
    uniqCombined64(20)(number)
FROM numbers(0);

SELECT
    cardinality,
    toUInt8(abs(hll12 / cardinality - 1) < 0.1),
    toUInt8(abs(combined12 / cardinality - 1) < 0.1),
    toUInt8(abs(combined13 / cardinality - 1) < 0.1),
    toUInt8(abs(combined20 / cardinality - 1) < 0.1),
    toUInt8(abs(combined64_12 / cardinality - 1) < 0.1),
    toUInt8(abs(combined64_13 / cardinality - 1) < 0.1),
    toUInt8(abs(combined64_20 / cardinality - 1) < 0.1)
FROM
(
    SELECT
        cardinality,
        uniqHLL12(number % cardinality) AS hll12,
        uniqCombined(12)(number % cardinality) AS combined12,
        uniqCombined(13)(number % cardinality) AS combined13,
        uniqCombined(20)(number % cardinality) AS combined20,
        uniqCombined64(12)(number % cardinality) AS combined64_12,
        uniqCombined64(13)(number % cardinality) AS combined64_13,
        uniqCombined64(20)(number % cardinality) AS combined64_20
    FROM numbers(100000)
    ARRAY JOIN [1, 1000, 10000, 100000] AS cardinality
    GROUP BY cardinality
)
ORDER BY cardinality;
