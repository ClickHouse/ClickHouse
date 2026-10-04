-- With `uuid_type_version = 2`, a bare `UUID` must resolve to `UUID2` also when it is hidden inside
-- a cast type literal of a persisted expression: the parser canonicalizes `CAST(x AS UUID)` and
-- `x::UUID` into `CAST(x, 'UUID')`, so the type name lives in a string literal rather than a type AST.
-- Column types inferred from such expressions (`DEFAULT` without an explicit type, `AS SELECT`)
-- previously materialized the historical `UUID` type regardless of the setting.

DROP TABLE IF EXISTS t_uuid2_default;
DROP TABLE IF EXISTS t_uuid2_as_select;
DROP TABLE IF EXISTS t_uuid2_operator;
DROP TABLE IF EXISTS t_uuid2_array;
DROP TABLE IF EXISTS t_uuid2_constant_type_name;
DROP TABLE IF EXISTS t_uuid2_alter;
DROP TABLE IF EXISTS t_uuid2_explicit;
DROP TABLE IF EXISTS t_uuid1_default;
DROP TABLE IF EXISTS t_uuid1_as_select;

SET uuid_type_version = 2;

SELECT 'inferred from DEFAULT expression';
CREATE TABLE t_uuid2_default (x DEFAULT CAST(generateUUIDv4() AS UUID)) ENGINE = Memory;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_default';
SHOW CREATE TABLE t_uuid2_default;

SELECT 'inferred from AS SELECT';
CREATE TABLE t_uuid2_as_select ENGINE = Memory AS SELECT CAST(generateUUIDv4() AS UUID) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_as_select';

SELECT 'inferred from AS SELECT with the cast operator';
CREATE TABLE t_uuid2_operator ENGINE = Memory AS SELECT generateUUIDv4()::UUID AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_operator';

SELECT 'nested type in the cast literal';
CREATE TABLE t_uuid2_array ENGINE = Memory AS SELECT CAST([generateUUIDv4()] AS Array(UUID)) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_array';

SELECT 'constant expression in the type-name argument';
CREATE TABLE t_uuid2_constant_type_name ENGINE = Memory AS SELECT CAST(generateUUIDv4(), concat('UU', 'ID')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_constant_type_name';

SELECT 'DEFAULT expression added by ALTER is materialized too';
CREATE TABLE t_uuid2_alter (k UInt8) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_uuid2_alter ADD COLUMN x UUID DEFAULT CAST(generateUUIDv4() AS UUID);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_alter' AND name = 'x';
SHOW CREATE TABLE t_uuid2_alter;

SELECT 'the explicit names UUID1 and UUID2 are never rewritten';
CREATE TABLE t_uuid2_explicit ENGINE = Memory AS SELECT CAST(generateUUIDv4() AS UUID1) AS x1, CAST(generateUUIDv4() AS UUID2) AS x2;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_explicit' ORDER BY name;

SET uuid_type_version = 1;

SELECT 'the historical type is unchanged under uuid_type_version = 1';
CREATE TABLE t_uuid1_default (x DEFAULT CAST(generateUUIDv4() AS UUID)) ENGINE = Memory;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid1_default';
CREATE TABLE t_uuid1_as_select ENGINE = Memory AS SELECT CAST(generateUUIDv4() AS UUID) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid1_as_select';

DROP TABLE t_uuid2_default;
DROP TABLE t_uuid2_as_select;
DROP TABLE t_uuid2_operator;
DROP TABLE t_uuid2_array;
DROP TABLE t_uuid2_constant_type_name;
DROP TABLE t_uuid2_alter;
DROP TABLE t_uuid2_explicit;
DROP TABLE t_uuid1_default;
DROP TABLE t_uuid1_as_select;
