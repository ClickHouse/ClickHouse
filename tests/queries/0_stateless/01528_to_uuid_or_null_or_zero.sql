DROP TABLE IF EXISTS to_uuid_test;

SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0T'); --{serverError 6}
SELECT toUUIDOrNull('61f0c404-5cb3-11e7-907b-a6006ad3dba0T');
SELECT toUUIDOrZero('59f0c404-5cb3-11e7-907b-a6006ad3dba0T');

CREATE TABLE to_uuid_test (value String) ENGINE = TinyLog();

INSERT INTO to_uuid_test VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID(value) FROM to_uuid_test; 

INSERT INTO to_uuid_test VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0T');
SELECT toUUID(value) FROM to_uuid_test; -- {serverError 6}
SELECT toUUIDOrNull(value) FROM to_uuid_test;
SELECT toUUIDOrZero(value) FROM to_uuid_test;

DROP TABLE to_uuid_test;

