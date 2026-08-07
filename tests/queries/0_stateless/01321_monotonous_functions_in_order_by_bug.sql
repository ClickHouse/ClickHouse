SELECT
    toStartOfHour(c1) AS _c1,
    c2
FROM values((toDateTime('2020-01-01 01:01:01'), 999), (toDateTime('2020-01-01 01:01:59'), 1))
ORDER BY
    _c1 ASC,
    c2 ASC
