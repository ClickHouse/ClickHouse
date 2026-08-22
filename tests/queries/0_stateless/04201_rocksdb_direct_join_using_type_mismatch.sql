-- Tags: use-rocksdb

DROP TABLE IF EXISTS rdb;
DROP TABLE IF EXISTS t2;

CREATE TABLE rdb (key UInt32, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO rdb SELECT number, toString(number) FROM numbers(10);

CREATE TABLE t2 (k UInt16) ENGINE = TinyLog;
INSERT INTO t2 SELECT number FROM numbers(10);

SET join_algorithm = 'direct';

-- USING with a mismatched join key type must surface as a user-facing error
-- (TYPE_MISMATCH or NOT_IMPLEMENTED), not as a server-side LOGICAL_ERROR.
SELECT * FROM (SELECT toUInt64(k) AS key FROM t2) AS t2
INNER JOIN rdb USING (key) ORDER BY key; -- { serverError NOT_IMPLEMENTED,TYPE_MISMATCH }

DROP TABLE rdb;
DROP TABLE t2;
