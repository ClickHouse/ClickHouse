-- https://github.com/ClickHouse/ClickHouse/issues/19311
WITH (SELECT groupBitmapState(number::Nullable(UInt8)) as n from numbers(1)) as n SELECT number as x, bitmapContains(n, x) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH
    men AS
        (
            SELECT
                toUInt32(number) AS id,
                mod(id, 100) AS age,
                mod(id, 1000) * 60 AS sal,
                mod(id, 60) AS nat
            FROM system.numbers
            LIMIT 10
        ),
    t AS
        (
            SELECT
                number AS n,
                multiIf(n = 0, 0, n = 1, 6, n = 2, 30, NULL) AS lo,
                multiIf(n = 0, 5, n = 1, 29, n = 2, 99, NULL) AS hi
            FROM system.numbers
            LIMIT 3
        ),
    t2 AS
        (
            SELECT
                toString(n) AS name,
                groupBitmapState(multiIf((age >= lo) AND (age <= hi), id, NULL)) AS bit_state
            FROM men, t
            GROUP BY n
        )
SELECT
    name,
    sumIf(sal, bitmapContains(bit_state, id))
FROM men, t2
GROUP BY name; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
