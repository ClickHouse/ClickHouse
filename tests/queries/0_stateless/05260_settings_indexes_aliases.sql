-- Settings spelled with `indices` also accept `indexes`.
-- https://github.com/ClickHouse/ClickHouse/issues/37288

SELECT name, alias_for FROM system.settings
WHERE name IN ('allow_suspicious_indices', 'ignore_data_skipping_indices', 'force_data_skipping_indices', 'secondary_indices_enable_bulk_filtering')
ORDER BY name;
