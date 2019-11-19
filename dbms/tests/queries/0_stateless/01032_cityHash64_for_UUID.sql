DROP TABLE IF EXISTS t_uuid;
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog;
INSERT INTO t_uuid SELECT generateUUIDv4();
INSERT INTO t_uuid SELECT generateUUIDv4();
SELECT count() FROM t_uuid WHERE cityHash64(x) = cityHash64(toString(x));
DROP TABLE IF EXISTS t_uuid;
