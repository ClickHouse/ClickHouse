-- Tags: no-fasttest
-- `MaxCompute` is not built in the fast-test image. No remote service is needed.
-- Regresses `MC-E2E-017l`: removing the last setting must leave valid table metadata.

SET allow_experimental_maxcompute_storage_engine = 1;

DROP TABLE IF EXISTS mc_reset_format;
DROP TABLE IF EXISTS mc_reset_format_raw;
DROP TABLE IF EXISTS mc_reset_format_default;

CREATE TABLE mc_reset_format (id Int64)
ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 'row';
CREATE TABLE mc_reset_format_raw (id Int64)
ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 'row';
CREATE TABLE mc_reset_format_default (id Int64)
ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret');

-- Resetting an unset setting is also valid.
ALTER TABLE mc_reset_format_default RESET SETTING maxcompute_read_format;
SELECT 'initial', count(), countIf(position(create_table_query, 'maxcompute_read_format') > 0),
    countIf(endsWith(trimRight(create_table_query), 'SETTINGS'))
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mc_reset_format', 'mc_reset_format_raw', 'mc_reset_format_default');

ALTER TABLE mc_reset_format RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_raw RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_default RESET SETTING maxcompute_read_format;

-- Repeated resets must still be accepted while the metadata settings container is empty.
ALTER TABLE mc_reset_format RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_raw RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_default RESET SETTING maxcompute_read_format;

SELECT 'reset', count(), countIf(position(create_table_query, 'maxcompute_read_format') > 0),
    countIf(endsWith(trimRight(create_table_query), 'SETTINGS'))
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mc_reset_format', 'mc_reset_format_raw', 'mc_reset_format_default');

-- Non-setting alterations must not restore a bare `SETTINGS` clause.
ALTER TABLE mc_reset_format COMMENT COLUMN id 'after reset';
ALTER TABLE mc_reset_format_raw COMMENT COLUMN id 'after reset';
ALTER TABLE mc_reset_format_default COMMENT COLUMN id 'after reset';

DETACH TABLE mc_reset_format SYNC;
ATTACH TABLE mc_reset_format;
DETACH TABLE mc_reset_format_raw SYNC;
ATTACH TABLE mc_reset_format_raw;
DETACH TABLE mc_reset_format_default SYNC;
ATTACH TABLE mc_reset_format_default;

SELECT 'reattach', count(), countIf(position(create_table_query, 'maxcompute_read_format') > 0),
    countIf(endsWith(trimRight(create_table_query), 'SETTINGS'))
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mc_reset_format', 'mc_reset_format_raw', 'mc_reset_format_default');

ALTER TABLE mc_reset_format RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_raw RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_default RESET SETTING maxcompute_read_format;

SELECT 'reset_after_reattach', count(), countIf(position(create_table_query, 'maxcompute_read_format') > 0),
    countIf(endsWith(trimRight(create_table_query), 'SETTINGS'))
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mc_reset_format', 'mc_reset_format_raw', 'mc_reset_format_default');

ALTER TABLE mc_reset_format MODIFY SETTING maxcompute_read_format = 'row';
ALTER TABLE mc_reset_format_raw MODIFY SETTING maxcompute_read_format = 'row';
ALTER TABLE mc_reset_format_default MODIFY SETTING maxcompute_read_format = 'row';

SELECT 'modify_again', count(),
    countIf(position(create_table_query, 'maxcompute_read_format = \'row\'') > 0)
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mc_reset_format', 'mc_reset_format_raw', 'mc_reset_format_default');

ALTER TABLE mc_reset_format RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_raw RESET SETTING maxcompute_read_format;
ALTER TABLE mc_reset_format_default RESET SETTING maxcompute_read_format;

SELECT 'reset_again', count(), countIf(position(create_table_query, 'maxcompute_read_format') > 0),
    countIf(endsWith(trimRight(create_table_query), 'SETTINGS'))
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mc_reset_format', 'mc_reset_format_raw', 'mc_reset_format_default');

DROP TABLE mc_reset_format;
DROP TABLE mc_reset_format_raw;
DROP TABLE mc_reset_format_default;
