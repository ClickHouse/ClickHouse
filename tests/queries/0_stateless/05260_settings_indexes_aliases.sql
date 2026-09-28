-- Settings spelled with `indices` also accept `indexes`.
-- https://github.com/ClickHouse/ClickHouse/issues/37288

SELECT name, alias_for FROM system.settings
WHERE name IN ('allow_suspicious_indexes', 'ignore_data_skipping_indexes', 'force_data_skipping_indexes', 'secondary_indexes_enable_bulk_filtering')
ORDER BY name;
