-- Regression test: INTERPOLATE () with aliased ORDER BY columns on old analyzer
-- https://github.com/ClickHouse/ClickHouse/issues/106248
SET use_with_fill_by_sorting_prefix = 1;
SET enable_analyzer = 0;
SELECT a AS aa, h, s FROM (
    SELECT 1::UInt8 AS a, toDateTime('2024-01-01 00:00:00') AS h, 'v1' AS s
    UNION ALL SELECT 1, toDateTime('2024-01-01 02:00:00'), 'v2'
    UNION ALL SELECT 2, toDateTime('2024-01-01 00:00:00'), 'v3'
    UNION ALL SELECT 2, toDateTime('2024-01-01 01:00:00'), 'v4'
) ORDER BY a, h ASC WITH FILL STEP INTERVAL 1 HOUR INTERPOLATE ();
SELECT a, h AS hh, s FROM (
    SELECT 1::UInt8 AS a, toDateTime('2024-01-01 00:00:00') AS h, 'v1' AS s
    UNION ALL SELECT 1, toDateTime('2024-01-01 02:00:00'), 'v2'
    UNION ALL SELECT 2, toDateTime('2024-01-01 00:00:00'), 'v3'
    UNION ALL SELECT 2, toDateTime('2024-01-01 01:00:00'), 'v4'
) ORDER BY a, h ASC WITH FILL STEP INTERVAL 1 HOUR INTERPOLATE ();
