-- Tests for the UUID2 data type (correctly-sorting variant of UUID) and the uuid_type_version setting.

SELECT '-- type names and alias';
SELECT toTypeName('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2);
SELECT toTypeName('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID1); -- UUID1 is an alias of UUID
SELECT toTypeName('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID);

SELECT '-- text round-trip is canonical';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS x, toString(x);

SELECT '-- UUID2 sorts lexicographically (unlike UUID)';
DROP TABLE IF EXISTS t_uuid2;
CREATE TABLE t_uuid2 (x UUID2) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_uuid2 VALUES ('00000002-0000-0000-0000-000000000000'), ('00000000-0000-0000-0002-000000000000'), ('00000000-0000-0000-0000-000000000009');
SELECT x FROM t_uuid2 ORDER BY x;
SELECT '-- same values as UUID would sort differently';
SELECT x FROM (SELECT arrayJoin(['00000002-0000-0000-0000-000000000000', '00000000-0000-0000-0002-000000000000', '00000000-0000-0000-0000-000000000009'])::UUID AS x) ORDER BY x;

SELECT '-- min/max';
SELECT min(x), max(x) FROM t_uuid2;

SELECT '-- conversions round-trip';
WITH '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS u2
SELECT
    u2 = CAST(CAST(u2 AS String) AS UUID2),
    u2 = CAST(CAST(u2 AS UInt128) AS UUID2),
    u2 = CAST(CAST(u2 AS UUID) AS UUID2);

SELECT '-- FixedString(16) (canonical bytes) parses to UUID2';
SELECT CAST(CAST(unhex('61f0c4045cb311e7907ba6006ad3dba0') AS FixedString(16)) AS UUID2);

SELECT '-- UUID2 -> UInt128 (identity) equals UUID -> UInt128 (half-swap): both give the canonical integer';
SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS UInt128) = CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID AS UInt128);

SELECT '-- comparison with string literal';
SELECT count() FROM t_uuid2 WHERE x = '00000000-0000-0000-0000-000000000009';

SELECT '-- setting materializes bare UUID as UUID2 (version 2), leaves UUID1/UUID2 explicit';
DROP TABLE IF EXISTS t_mat;
SET uuid_type_version = 2;
CREATE TABLE t_mat (a UUID, b UUID1, c UUID2, d Array(UUID), e Nullable(UUID)) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_mat' ORDER BY name;
SELECT '-- version 1 (default) leaves UUID as UUID';
SET uuid_type_version = 1;
DROP TABLE IF EXISTS t_mat1;
CREATE TABLE t_mat1 (a UUID, d Array(UUID)) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_mat1' ORDER BY name;

DROP TABLE t_uuid2;
DROP TABLE t_mat;
DROP TABLE t_mat1;
