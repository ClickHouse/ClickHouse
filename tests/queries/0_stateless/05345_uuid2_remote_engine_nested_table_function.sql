-- The `Remote` and `RemoteSecure` engines take the arguments of the `remote` table function, including a nested
-- table function with a structure string. `uuid_type_version` must be materialized into that persisted structure
-- at creation time, as for the `remote` table function itself.

DROP TABLE IF EXISTS uuid2_remote_v1;
DROP TABLE IF EXISTS uuid2_remote_v2;
DROP TABLE IF EXISTS uuid2_remote_named;

SET uuid_type_version = 1;
CREATE TABLE uuid2_remote_v1 ENGINE = Remote('127.0.0.1', format('CSV', 'id UUID, v UInt8', '61f0c404-5cb3-11e7-907b-a6006ad3dba0,1'));

SET uuid_type_version = 2;
CREATE TABLE uuid2_remote_v2 ENGINE = Remote('127.0.0.1', format('CSV', 'id UUID, v UInt8', '61f0c404-5cb3-11e7-907b-a6006ad3dba0,1'));

SELECT name, position(engine_full, 'UUID2') > 0 FROM system.tables WHERE database = currentDatabase() AND name LIKE 'uuid2_remote_v%' ORDER BY name;

-- The stored definitions keep their types whatever the setting is when they are executed.
SET uuid_type_version = 2;
SELECT 'v1 under 2', toTypeName(id), id FROM uuid2_remote_v1;
SET uuid_type_version = 1;
SELECT 'v2 under 1', toTypeName(id), id FROM uuid2_remote_v2;

DROP TABLE uuid2_remote_v1;
DROP TABLE uuid2_remote_v2;
