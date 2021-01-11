SELECT cityHash64(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) AS uuid;
DROP TABLE IF EXISTS t_uuid;
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog;
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
SELECT (SELECT count() FROM t_uuid WHERE cityHash64(reinterpretAsString(x)) = cityHash64(x) and length(reinterpretAsString(x)) = 16) = (SELECT count() AS c2 FROM t_uuid WHERE length(reinterpretAsString(x)) = 16);
DROP TABLE IF EXISTS t_uuid;
