-- Under `uuid_type_version = 2`, the schema string of a table function in a persisted definition is materialized
-- with `UUID2`. A scalar function nested in a table function argument is not a table function, even when it shares
-- its name with one: the three-argument string formatting `format` must be left alone. A table function nested in
-- a wrapper table function (`loop`, `remote`) is still materialized.

SET uuid_type_version = 2;

DROP TABLE IF EXISTS t_uuid2_scalar_format;
DROP TABLE IF EXISTS t_uuid2_remote_sharding_key;
DROP TABLE IF EXISTS t_uuid2_remote_src;
DROP VIEW IF EXISTS v_uuid2_loop;
DROP VIEW IF EXISTS v_uuid2_remote;

CREATE TABLE t_uuid2_scalar_format AS file(format('{}_{}', 'id UUID', 'data.csv'), 'CSV', 'k UInt8');
SELECT 'scalar format', position(create_table_query, 'id UUID_data.csv') > 0, position(create_table_query, 'UUID2') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_scalar_format';

-- The sharding key of `remote` is a scalar expression too, only the second argument can be a table function.
CREATE TABLE t_uuid2_remote_src (id UInt8) ENGINE = Memory;
CREATE TABLE t_uuid2_remote_sharding_key (id UInt8) AS remote('127.0.0.1', currentDatabase(), 't_uuid2_remote_src', format('{}{}', 'id UUID', 'x'));
SELECT 'remote sharding key', position(create_table_query, '\'id UUID\'') > 0, position(create_table_query, 'UUID2') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_remote_sharding_key';

CREATE VIEW v_uuid2_loop AS SELECT * FROM loop(format('CSV', 'id UUID', '61f0c404-5cb3-11e7-907b-a6006ad3dba0')) LIMIT 1;
SELECT 'loop', toTypeName(id), id FROM v_uuid2_loop;

CREATE VIEW v_uuid2_remote AS SELECT * FROM remote('127.0.0.1', format('CSV', 'id UUID', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT 'remote', toTypeName(id), id FROM v_uuid2_remote;

DROP TABLE t_uuid2_scalar_format;
DROP TABLE t_uuid2_remote_sharding_key;
DROP TABLE t_uuid2_remote_src;
DROP VIEW v_uuid2_loop;
DROP VIEW v_uuid2_remote;
