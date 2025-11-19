DROP TABLE IF EXISTS to_uuid_test;

SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0T'); --{serverError CANNOT_PARSE_TEXT}
SELECT toUUIDOrNull('61f0c404-5cb3-11e7-907b-a6006ad3dba0T');
SELECT toUUIDOrZero('59f0c404-5cb3-11e7-907b-a6006ad3dba0T');

CREATE TABLE to_uuid_test (value String) ENGINE = TinyLog();

INSERT INTO to_uuid_test VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID(value) FROM to_uuid_test; 

INSERT INTO to_uuid_test VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0T');
-- If the Memory engine is replaced by MergeTree, this query returns a result for the first row
-- but throws an error while processing the second row.
-- ORDER BY ALL ensures consistent results between engines.
SELECT toUUID(value) FROM to_uuid_test ORDER BY ALL; -- {serverError CANNOT_PARSE_TEXT}
SELECT toUUIDOrNull(value) FROM to_uuid_test ORDER BY ALL;
SELECT toUUIDOrZero(value) FROM to_uuid_test ORDER BY ALL;

DROP TABLE to_uuid_test;

