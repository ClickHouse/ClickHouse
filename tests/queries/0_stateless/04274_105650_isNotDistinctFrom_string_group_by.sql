-- Smoke test for #105650: `<=>` over a String GROUP BY key with WITH ROLLUP
-- must not touch zero-row placeholder columns during header-time partial
-- evaluation.  See PR description for the full plan path.

DROP TABLE IF EXISTS _s1_105650;
CREATE TABLE _s1_105650 (
    a UInt32, b Nullable(Int64), c String, d Float64,
    e Nullable(String), f Date, g UInt8, h Nullable(UInt16)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO _s1_105650 SELECT
    number,
    if(number%3=0, NULL, number*10),
    toString(number%7),
    number*0.1-50,
    if(number%7=0, NULL, toString(number)),
    toDate('2020-01-01') + number,
    number%3,
    if(number%5=0, NULL, toUInt16(number))
FROM numbers(500);

-- Original fuzzer query.  We only check that it does not crash; the body of
-- the result is intentionally projected away because
-- `groupArrayMovingAvgStateDistinct...` state encoding is not stable
-- between builds.
SELECT count() FROM (
    SELECT
        c,
        groupArrayMovingAvgStateDistinctOrDefaultDistinctOrNull(b)
    FROM _s1_105650
    GROUP BY
        c <=> '65535',
        b <= materialize(toUInt128(1048577)),
        c
    WITH ROLLUP
    HAVING 2147483648 > countOrNullArgMinOrDefault(
        b,
        toString(-9223372036854775808, NULL,
            toString(-2147483649,
                toString(toString(toNullable(NULL)), NULL),
                divide(-9223372036854775807 = a, a > materialize(2147483647))
            )
        )
    )
);

-- Minimised form that hits the same plan path: `isNotDistinctFrom` over a
-- String GROUP BY key with WITH ROLLUP.  Stable result, one extra rollup
-- row.
SELECT c IS NOT DISTINCT FROM '65535' AS k, count() FROM _s1_105650
GROUP BY c <=> '65535', c WITH ROLLUP ORDER BY k, count();

DROP TABLE _s1_105650;
