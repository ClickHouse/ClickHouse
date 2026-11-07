-- With `uuid_type_version = 2`, a bare `UUID` must be materialized also when the persisted type name or
-- schema string is built from a constant string expression rather than written as a plain literal. The
-- normalizer folds a whitelist of deterministic string functions (`concat`, `replaceOne`, `replaceAll`,
-- `upper`, `lower` and their aliases) into a literal before the substitution, mirroring their runtime
-- semantics byte for byte. Anything outside the whitelist is left alone.

DROP TABLE IF EXISTS t_fold_replace_one;
DROP TABLE IF EXISTS t_fold_replace_all;
DROP TABLE IF EXISTS t_fold_upper;
DROP TABLE IF EXISTS t_fold_nested;
DROP TABLE IF EXISTS t_fold_json_extract;
DROP TABLE IF EXISTS t_fold_outside_whitelist;
DROP VIEW IF EXISTS v_fold_protobuf;
DROP VIEW IF EXISTS v_fold_capnproto;
DROP VIEW IF EXISTS v_fold_table_function;
DROP TABLE IF EXISTS t_fold_v1;

SET uuid_type_version = 2;

SELECT 'replaceOne in the type-name argument';
CREATE TABLE t_fold_replace_one ENGINE = Memory AS SELECT CAST(generateUUIDv4(), replaceOne('x UUID', 'x ', '')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_replace_one';

SELECT 'replaceAll in the type-name argument';
CREATE TABLE t_fold_replace_all ENGINE = Memory AS SELECT CAST(generateUUIDv4(), replaceAll('U_U_I_D', '_', '')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_replace_all';

SELECT 'upper in the type-name argument';
CREATE TABLE t_fold_upper ENGINE = Memory AS SELECT CAST(generateUUIDv4(), upper('uuid')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_upper';

SELECT 'nested fold';
CREATE TABLE t_fold_nested ENGINE = Memory AS SELECT CAST(generateUUIDv4(), replaceOne(concat('x ', upper('uu'), 'ID'), 'x ', '')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_nested';

SELECT 'JSONExtract with a folded type name';
CREATE TABLE t_fold_json_extract ENGINE = Memory
    AS SELECT JSONExtract('{"a": "61f0c404-5cb3-11e7-907b-a6006ad3dba0"}', 'a', replaceOne('x UUID', 'x ', '')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_json_extract';

SELECT 'structureToProtobufSchema schema string';
CREATE VIEW v_fold_protobuf AS SELECT structureToProtobufSchema(concat('id ', 'UUID'), 'MessageType') AS s;
SELECT create_table_query LIKE '%UUID2%' FROM system.tables WHERE database = currentDatabase() AND name = 'v_fold_protobuf';

SELECT 'structureToCapnProtoSchema schema string';
CREATE VIEW v_fold_capnproto AS SELECT structureToCapnProtoSchema(replaceOne('x UUID', 'x', 'id'), 'MessageType') AS s;
SELECT create_table_query LIKE '%UUID2%' FROM system.tables WHERE database = currentDatabase() AND name = 'v_fold_capnproto';

SELECT 'table-function structure string';
CREATE VIEW v_fold_table_function AS SELECT * FROM format('CSV', replaceOne('x UUID', 'x', 'id'), '"61f0c404-5cb3-11e7-907b-a6006ad3dba0"');
SELECT create_table_query LIKE '%UUID2%' FROM system.tables WHERE database = currentDatabase() AND name = 'v_fold_table_function';
SELECT toTypeName(id) FROM v_fold_table_function;

SELECT 'an expression outside the whitelist is left alone';
CREATE TABLE t_fold_outside_whitelist ENGINE = Memory AS SELECT CAST(generateUUIDv4(), trim(' UUID ')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_outside_whitelist';

SET uuid_type_version = 1;

SELECT 'nothing is folded or rewritten under uuid_type_version = 1';
CREATE TABLE t_fold_v1 ENGINE = Memory AS SELECT CAST(generateUUIDv4(), replaceOne('x UUID', 'x ', '')) AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_fold_v1';

DROP TABLE t_fold_replace_one;
DROP TABLE t_fold_replace_all;
DROP TABLE t_fold_upper;
DROP TABLE t_fold_nested;
DROP TABLE t_fold_json_extract;
DROP TABLE t_fold_outside_whitelist;
DROP VIEW v_fold_protobuf;
DROP VIEW v_fold_capnproto;
DROP VIEW v_fold_table_function;
DROP TABLE t_fold_v1;
