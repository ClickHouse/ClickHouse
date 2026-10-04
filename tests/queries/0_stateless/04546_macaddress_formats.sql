-- `MacAddress` support in the columnar formats and in `JSONExtract` / `generateRandom`.
SET allow_experimental_macaddress_type = 1;

DROP TABLE IF EXISTS t_macaddress_formats;
CREATE TABLE t_macaddress_formats
(
    id UInt32,
    mac MacAddress
) ENGINE = Memory;

INSERT INTO t_macaddress_formats VALUES (1, '00:1a:2b:3c:4d:5e'), (2, 'ff:ff:ff:ff:ff:ff'), (3, '00:00:00:00:00:00');

-- The native writer/reader (`ArrowIPC`) and the Apache Arrow library path are separate
-- implementations, so exercise both.
SELECT 'Arrow round-trip, native writer/reader';
INSERT INTO FUNCTION file('04546_mac_native.arrow', 'Arrow', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id
SETTINGS output_format_arrow_use_native_writer = 1;
SELECT id, mac FROM file('04546_mac_native.arrow', 'Arrow', 'id UInt32, mac MacAddress') ORDER BY id
SETTINGS input_format_arrow_use_native_reader = 1;

SELECT 'Arrow round-trip, Apache Arrow library';
INSERT INTO FUNCTION file('04546_mac_lib.arrow', 'Arrow', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id
SETTINGS output_format_arrow_use_native_writer = 0;
SELECT id, mac FROM file('04546_mac_lib.arrow', 'Arrow', 'id UInt32, mac MacAddress') ORDER BY id
SETTINGS input_format_arrow_use_native_reader = 0;

SELECT 'Parquet round-trip';
INSERT INTO FUNCTION file('04546_mac.parquet', 'Parquet', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id;
SELECT id, mac FROM file('04546_mac.parquet', 'Parquet', 'id UInt32, mac MacAddress') ORDER BY id;

SELECT 'ORC round-trip';
INSERT INTO FUNCTION file('04546_mac.orc', 'ORC', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id;
SELECT id, mac FROM file('04546_mac.orc', 'ORC', 'id UInt32, mac MacAddress') ORDER BY id;

SELECT 'Avro round-trip';
INSERT INTO FUNCTION file('04546_mac.avro', 'Avro', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id;
SELECT id, mac FROM file('04546_mac.avro', 'Avro', 'id UInt32, mac MacAddress') ORDER BY id;

SELECT 'MsgPack round-trip';
INSERT INTO FUNCTION file('04546_mac.msgpack', 'MsgPack', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id;
SELECT id, mac FROM file('04546_mac.msgpack', 'MsgPack', 'id UInt32, mac MacAddress') ORDER BY id;

SELECT 'BSONEachRow round-trip';
INSERT INTO FUNCTION file('04546_mac.bson', 'BSONEachRow', 'id UInt32, mac MacAddress') SELECT id, mac FROM t_macaddress_formats ORDER BY id;
SELECT id, mac FROM file('04546_mac.bson', 'BSONEachRow', 'id UInt32, mac MacAddress') ORDER BY id;

SELECT 'JSONExtract';
SELECT JSONExtract('{"mac": "00:1a:2b:3c:4d:5e"}', 'mac', 'MacAddress');
SELECT JSONExtract('{"mac": "not-a-mac"}', 'mac', 'MacAddress');

SELECT 'generateRandom produces varied values that keep the 48-bit invariant';
SELECT count(), countDistinct(mac) > 1, max(CAST(mac AS UInt64)) <= 0xFFFFFFFFFFFF
FROM (SELECT mac FROM generateRandom('mac MacAddress', 42) LIMIT 1000);

DROP TABLE t_macaddress_formats;
