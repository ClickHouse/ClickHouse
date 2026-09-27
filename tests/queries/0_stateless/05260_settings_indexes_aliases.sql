-- Settings spelled with `indices` also accept `indexes`.
-- https://github.com/ClickHouse/ClickHouse/issues/37288

SELECT name, alias_for FROM system.settings
WHERE name IN ('allow_suspicious_indexes', 'ignore_data_skipping_indexes', 'force_data_skipping_indexes', 'secondary_indexes_enable_bulk_filtering')
ORDER BY name;

DROP TABLE IF EXISTS t_indexes_aliases;

CREATE TABLE t_indexes_aliases (k UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_indexes_aliases SELECT number, number FROM numbers(10);

SELECT count() FROM t_indexes_aliases WHERE v = 5 SETTINGS force_data_skipping_indexes = 'idx_v';
SELECT count() FROM t_indexes_aliases WHERE k = 5 SETTINGS force_data_skipping_indexes = 'idx_v'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_indexes_aliases WHERE v = 5 SETTINGS ignore_data_skipping_indexes = 'idx_v', force_data_skipping_indexes = 'idx_v'; -- { serverError INDEX_NOT_USED }

SET allow_suspicious_indexes = 1;
SELECT value FROM system.settings WHERE name = 'allow_suspicious_indices';

SET secondary_indexes_enable_bulk_filtering = 0;
SELECT value FROM system.settings WHERE name = 'secondary_indices_enable_bulk_filtering';

DROP TABLE t_indexes_aliases;
