-- https://github.com/ClickHouse/ClickHouse/issues/60317
SELECT
    toNullable(materialize(_CAST(30, 'LowCardinality(UInt8)'))) as a,
    _CAST(30, 'LowCardinality(UInt8)') as b,
    makeDate(materialize(_CAST(30, 'LowCardinality(UInt8)')), 10, _CAST(30, 'Nullable(UInt8)')) as c
FROM system.one
GROUP BY
    _CAST(30, 'Nullable(UInt8)')
SETTINGS allow_experimental_analyzer = 1;
