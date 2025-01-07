-- https://github.com/ClickHouse/ClickHouse/issues/60317
SELECT
    toNullable(materialize(_CAST(30, 'LowCardinality(UInt8)'))) as a,
    _CAST(30, 'LowCardinality(UInt8)') as b,
    makeDate(materialize(_CAST(30, 'LowCardinality(UInt8)')), 10, _CAST(30, 'Nullable(UInt8)')) as c
FROM system.one
GROUP BY
    _CAST(30, 'Nullable(UInt8)')
SETTINGS enable_analyzer = 1;

-- WITH CUBE (note that result is different with the analyzer (analyzer is correct including all combinations)
SELECT
    toNullable(toNullable(materialize(_CAST(30, 'LowCardinality(UInt8)')))) AS `toNullable(toNullable(materialize(toLowCardinality(30))))`,
    _CAST(0, 'Date') AS `makeDate(-1980.1, -1980.1, 10)`,
    _CAST(30, 'LowCardinality(UInt8)') AS `toLowCardinality(30)`,
    30 AS `30`,
    makeDate(materialize(_CAST(30, 'LowCardinality(UInt8)')), 10, _CAST(30, 'Nullable(UInt8)')) AS `makeDate(materialize(toLowCardinality(30)), 10, toNullable(toNullable(30)))`,
    -1980.1 AS `-1980.1`
FROM system.one AS __table1
GROUP BY
    _CAST(30, 'Nullable(UInt8)'),
    -1980.1,
    materialize(30),
    _CAST(30, 'Nullable(UInt8)')
WITH CUBE
WITH TOTALS
SETTINGS enable_analyzer = 1;
