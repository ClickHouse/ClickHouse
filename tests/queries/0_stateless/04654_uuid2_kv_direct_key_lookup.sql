-- Tags: no-fasttest, use-rocksdb
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the rocksdb engine is not enabled by default

-- `UUID` and `UUID2` share the `Field` representation but store the two 64-bit halves in a different order.
-- A key-value point lookup extracts the key constant from the filter and serializes it directly, so it has to
-- reconcile the layout of the constant with the layout of the key column, otherwise the direct `GetKeys` read
-- returns no row for a key that exists.

DROP TABLE IF EXISTS 04654_kv;
DROP TABLE IF EXISTS 04654_kv_composite;
DROP TABLE IF EXISTS 04654_kv_bare;

CREATE TABLE 04654_kv (key UUID2, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO 04654_kv VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'a'), ('00112233-4455-6677-8899-aabbccddeeff', 'b');

SELECT '-- single key, constant typed as UUID2';
SELECT * FROM 04654_kv WHERE key = toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0');

SELECT '-- single key, constant typed as UUID';
SELECT * FROM 04654_kv WHERE key = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');

SELECT '-- single key, IN with constants typed as UUID';
SELECT * FROM 04654_kv WHERE key IN (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('00112233-4455-6677-8899-aabbccddeeff')) ORDER BY value;

SELECT '-- single key, IN with a subquery producing UUID';
SELECT * FROM 04654_kv WHERE key IN (SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) ORDER BY value;

SELECT '-- single key, absent value stays absent';
SELECT count() FROM 04654_kv WHERE key = toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff');

CREATE TABLE 04654_kv_composite (k1 UUID2, k2 UUID2, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO 04654_kv_composite VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', '00112233-4455-6677-8899-aabbccddeeff', 'a');

SELECT '-- composite key, tuple equality with constants typed as UUID';
SELECT * FROM 04654_kv_composite WHERE (k1, k2) = (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('00112233-4455-6677-8899-aabbccddeeff'));

SELECT '-- composite key, tuple IN with constants typed as UUID';
SELECT * FROM 04654_kv_composite WHERE (k1, k2) IN ((toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('00112233-4455-6677-8899-aabbccddeeff')));

SELECT '-- composite key, per-column equality with constants typed as UUID';
SELECT * FROM 04654_kv_composite WHERE k1 = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AND k2 = toUUID('00112233-4455-6677-8899-aabbccddeeff');

SET uuid_type_version = 2;

CREATE TABLE 04654_kv_bare (key UUID, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
SELECT '-- bare UUID materialized to UUID2';
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = '04654_kv_bare' AND name = 'key';

INSERT INTO 04654_kv_bare VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'a');

SELECT '-- bare UUID key, constant typed as UUID';
SELECT * FROM 04654_kv_bare WHERE key = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');

DROP TABLE 04654_kv;
DROP TABLE 04654_kv_composite;
DROP TABLE 04654_kv_bare;
