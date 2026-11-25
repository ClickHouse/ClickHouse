-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- no-fasttest: rocksdb is not enabled in fasttest.

-- A composite-key point lookup must not lose a candidate key. When one side of an `OR` names its rows
-- through a set whose element type the key type cannot hold, that side has to be decided by a full
-- scan: dropping the keys it names silently answers with fewer rows than a table without a key lookup.

DROP TABLE IF EXISTS t_kv_or_in;
DROP TABLE IF EXISTS t_mem_or_in;
DROP TABLE IF EXISTS t_set_dt;
DROP TABLE IF EXISTS t_set_dt64;

CREATE TABLE t_kv_or_in (dt DateTime('UTC'), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (dt, id);
CREATE TABLE t_mem_or_in (dt DateTime('UTC'), id UInt64) ENGINE = Memory;
INSERT INTO t_kv_or_in VALUES ('2024-01-02 00:00:00', 2), ('2024-01-03 00:00:00', 3);
INSERT INTO t_mem_or_in VALUES ('2024-01-02 00:00:00', 2), ('2024-01-03 00:00:00', 3);

CREATE TABLE t_set_dt (t DateTime('UTC'), id UInt64) ENGINE = Memory;
INSERT INTO t_set_dt VALUES ('2024-01-03 00:00:00', 3);
CREATE TABLE t_set_dt64 (t DateTime64(3, 'UTC'), id UInt64) ENGINE = Memory;
INSERT INTO t_set_dt64 VALUES ('2024-01-02 00:00:00', 2);

-- The set the key type can hold comes first, so its key is already collected when the other is refused.
SELECT count() FROM t_kv_or_in WHERE (dt, id) IN (SELECT t, id FROM t_set_dt) OR (dt, id) IN (SELECT t, id FROM t_set_dt64);
SELECT count() FROM t_mem_or_in WHERE (dt, id) IN (SELECT t, id FROM t_set_dt) OR (dt, id) IN (SELECT t, id FROM t_set_dt64);

-- The other order, and with the refused side in the middle of three.
SELECT count() FROM t_kv_or_in WHERE (dt, id) IN (SELECT t, id FROM t_set_dt64) OR (dt, id) IN (SELECT t, id FROM t_set_dt);
SELECT count() FROM t_kv_or_in WHERE (dt, id) IN (SELECT t, id FROM t_set_dt) OR (dt, id) IN (SELECT t, id FROM t_set_dt64) OR (dt, id) IN (SELECT t, id FROM t_set_dt WHERE id = 3);

-- Positive control: when both sides hold the key type, both keys are found and the read stays a point
-- lookup instead of falling back to a scan.
SELECT count() FROM t_kv_or_in WHERE (dt, id) IN (SELECT t, id FROM t_set_dt) OR (dt, id) IN (SELECT toDateTime('2024-01-02 00:00:00', 'UTC'), toUInt64(2));
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_kv_or_in WHERE (dt, id) IN (SELECT t, id FROM t_set_dt) OR (dt, id) IN (SELECT toDateTime('2024-01-02 00:00:00', 'UTC'), toUInt64(2))) WHERE explain ILIKE '%ReadType: GetKeys%';

DROP TABLE t_kv_or_in;
DROP TABLE t_mem_or_in;
DROP TABLE t_set_dt;
DROP TABLE t_set_dt64;
