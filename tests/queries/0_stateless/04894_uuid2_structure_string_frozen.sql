-- The schema of a table function given as a whole columns declaration list in a string is a persisted schema
-- carrier: it is stored verbatim and reparsed on every execution. `uuid_type_version` must be materialized into
-- it once, at creation time, so that an already persisted definition cannot change its types later.

DROP VIEW IF EXISTS uuid2_frozen_v1;
DROP VIEW IF EXISTS uuid2_frozen_v2;
DROP VIEW IF EXISTS uuid2_frozen_generate;
DROP VIEW IF EXISTS uuid2_frozen_values;
DROP VIEW IF EXISTS uuid2_frozen_format_data;
DROP VIEW IF EXISTS uuid2_frozen_format_expression;
DROP TABLE IF EXISTS uuid2_frozen_as;

-- Created under version 1: the stored structure keeps the historical `UUID`.
SET uuid_type_version = 1;
CREATE VIEW uuid2_frozen_v1 AS SELECT * FROM format('CSV', 'id UUID, v UInt8', '61f0c404-5cb3-11e7-907b-a6006ad3dba0,1');
SELECT position(create_table_query, 'UUID2') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_frozen_v1';

-- Created under version 2: the stored structure is materialized as `UUID2`.
SET uuid_type_version = 2;
CREATE VIEW uuid2_frozen_v2 AS SELECT * FROM format('CSV', 'id UUID, v UInt8', '61f0c404-5cb3-11e7-907b-a6006ad3dba0,1');
SELECT position(create_table_query, 'UUID2') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_frozen_v2';

-- Executing a definition never resolves a bare `UUID` through the setting, so neither view changes its types
-- when the setting changes.
SET uuid_type_version = 2;
SELECT 'v1 under 2', toTypeName(id), id FROM uuid2_frozen_v1;
SET uuid_type_version = 1;
SELECT 'v2 under 1', toTypeName(id), id FROM uuid2_frozen_v2;

-- The same for `CREATE TABLE ... AS <table function>` and for the other table functions taking a structure.
SET uuid_type_version = 2;
CREATE TABLE uuid2_frozen_as AS format('CSV', 'id UUID, v UInt8', '61f0c404-5cb3-11e7-907b-a6006ad3dba0,1');
SELECT 'as table function', type FROM system.columns WHERE database = currentDatabase() AND table = 'uuid2_frozen_as' AND name = 'id';
CREATE VIEW uuid2_frozen_generate AS SELECT * FROM generateRandom('id UUID');
SET uuid_type_version = 1;
SELECT 'generateRandom', toTypeName(id) FROM uuid2_frozen_generate LIMIT 1;

-- `values` persists its first argument as a structure string as well.
SET uuid_type_version = 2;
CREATE VIEW uuid2_frozen_values AS SELECT * FROM values('id UUID', (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')));
SET uuid_type_version = 1;
SELECT 'values', toTypeName(id) FROM uuid2_frozen_values;

-- The two-argument overload of `format` has no structure argument: its last string is data, even when it looks
-- like a columns list. In contrast, the three-argument overload must fold and freeze an expression-built schema.
SET uuid_type_version = 2;
CREATE VIEW uuid2_frozen_format_data AS SELECT * FROM format('LineAsString', 'id UUID');
SELECT position(create_table_query, 'UUID2') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_frozen_format_data';
SELECT 'format data', line FROM uuid2_frozen_format_data;
CREATE VIEW uuid2_frozen_format_expression AS SELECT * FROM format('CSV', concat('id ', 'UUID'), '61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SET uuid_type_version = 1;
SELECT 'format expression', toTypeName(id) FROM uuid2_frozen_format_expression;

-- A string literal that is not a table-function schema must be left alone.
SELECT 'plain literal', 'id UUID' AS s;

DROP VIEW uuid2_frozen_v1;
DROP VIEW uuid2_frozen_v2;
DROP VIEW uuid2_frozen_generate;
DROP VIEW uuid2_frozen_values;
DROP VIEW uuid2_frozen_format_data;
DROP VIEW uuid2_frozen_format_expression;
DROP TABLE uuid2_frozen_as;
