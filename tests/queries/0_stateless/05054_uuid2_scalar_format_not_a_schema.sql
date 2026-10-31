-- `uuid_type_version = 2` materializes the schema string of a table function in a persisted definition,
-- and `format(format_name, structure, data)` is one such carrier. But `format` is also the regular scalar
-- string-formatting function `format(pattern, ...)`, so the rewrite must be driven by the position of the
-- call, not by its name: a persisted scalar `format` call keeps all of its arguments verbatim.

SET uuid_type_version = 2;

DROP TABLE IF EXISTS uuid2_scalar_format_05054;
DROP TABLE IF EXISTS uuid2_table_format_05054;
DROP TABLE IF EXISTS uuid2_as_format_05054;

SELECT '-- a scalar `format` call keeps its data arguments';
CREATE VIEW uuid2_scalar_format_05054 AS SELECT format('{} {}', 'id UUID', 'x') AS s;
SELECT create_table_query LIKE '%UUID2%' FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_scalar_format_05054';
SELECT s FROM uuid2_scalar_format_05054;

SELECT '-- the `format` table function still has its schema materialized';
CREATE VIEW uuid2_table_format_05054 AS SELECT * FROM format('CSV', 'id UUID', '61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT create_table_query LIKE '%UUID2%' FROM system.tables WHERE database = currentDatabase() AND name = 'uuid2_table_format_05054';
SELECT toTypeName(id) FROM uuid2_table_format_05054;

SELECT '-- ... including in the `CREATE TABLE ... AS <table function>` position';
CREATE TABLE uuid2_as_format_05054 AS format('CSV', 'id UUID', '61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uuid2_as_format_05054' AND name = 'id';

DROP TABLE uuid2_scalar_format_05054;
DROP TABLE uuid2_table_format_05054;
DROP TABLE uuid2_as_format_05054;
