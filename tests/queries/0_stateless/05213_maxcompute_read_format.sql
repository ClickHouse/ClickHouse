-- Tags: no-fasttest
-- `MaxCompute` is not built in the fast-test image. These checks require no remote service.

SET allow_experimental_maxcompute_storage_engine = 1;

DROP TABLE IF EXISTS mc_format_default;
DROP TABLE IF EXISTS mc_format_column;
DROP TABLE IF EXISTS mc_format_raw;
DROP TABLE IF EXISTS mc_format_invalid;

SELECT getSetting('maxcompute_read_format');

CREATE TABLE mc_format_default (id Int64)
ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret');
CREATE TABLE mc_format_column (id Int64)
ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 'column';
CREATE TABLE mc_format_raw (id Int64)
ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 'row';

SELECT name, position(create_table_query, 'maxcompute_read_format') > 0
FROM system.tables
WHERE database = currentDatabase() AND name IN ('mc_format_default', 'mc_format_column', 'mc_format_raw')
ORDER BY name;

SET maxcompute_read_format = 'column';
SELECT getSetting('maxcompute_read_format') SETTINGS maxcompute_read_format = 'row';
SELECT getSetting('maxcompute_read_format');
SET maxcompute_read_format = 'inherit';

ALTER TABLE mc_format_default MODIFY SETTING maxcompute_read_format = 'row';
ALTER TABLE mc_format_column MODIFY SETTING maxcompute_read_format = 'row';
ALTER TABLE mc_format_raw MODIFY SETTING maxcompute_read_format = 'column';
SELECT name, position(create_table_query, 'maxcompute_read_format = \'row\'') > 0,
    position(create_table_query, 'maxcompute_read_format = \'column\'') > 0
FROM system.tables
WHERE database = currentDatabase() AND name IN ('mc_format_column', 'mc_format_raw')
ORDER BY name;

CREATE TABLE mc_format_invalid (id Int64)
ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 'bogus'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE mc_format_invalid (id Int64)
ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 'inherit'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE mc_format_invalid (id Int64)
ENGINE = MaxCompute('https://tunnel.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_unknown_setting = 1; -- { serverError UNKNOWN_SETTING }
CREATE TABLE mc_format_invalid (id Int64)
ENGINE = MaxComputeRaw('https://odps.example', 'project', 'table', '', 'access_id', 'secret')
SETTINGS maxcompute_read_format = 1; -- { serverError BAD_GET }
ALTER TABLE mc_format_column MODIFY SETTING maxcompute_read_format = 'bogus'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mc_format_raw MODIFY SETTING maxcompute_read_format = 'inherit'; -- { serverError BAD_ARGUMENTS }

SELECT name, position(create_table_query, 'maxcompute_read_format = \'row\'') > 0,
    position(create_table_query, 'maxcompute_read_format = \'column\'') > 0
FROM system.tables
WHERE database = currentDatabase() AND name IN ('mc_format_column', 'mc_format_raw')
ORDER BY name;

ALTER TABLE mc_format_default RESET SETTING maxcompute_read_format;
ALTER TABLE mc_format_column RESET SETTING maxcompute_read_format;
ALTER TABLE mc_format_raw RESET SETTING maxcompute_read_format;
SELECT countIf(position(create_table_query, 'maxcompute_read_format') > 0)
FROM system.tables
WHERE database = currentDatabase() AND name IN ('mc_format_default', 'mc_format_column', 'mc_format_raw');

DROP TABLE mc_format_default;
DROP TABLE mc_format_column;
DROP TABLE mc_format_raw;
