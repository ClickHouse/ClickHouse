-- Tags: no-replicated-database, no-ordinary-database, no-async-insert
-- no-replicated-database: TRUNCATE is a distributed DDL there, and those are not supported inside a transaction
-- no-ordinary-database: transactions need an Atomic database
-- no-async-insert: async inserts compute deduplication block ids differently

-- With `non_replicated_deduplication_window`, a block whose part was removed by REPLACE PARTITION (destination),
-- MOVE PARTITION TO TABLE (source) or TRUNCATE inside a transaction is written again by a later INSERT, as after
-- DROP PARTITION. A repeated INSERT is still deduplicated, and so is a block of a partition that was not touched.
-- https://github.com/ClickHouse/ClickHouse/issues/122805

DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS msrc;
DROP TABLE IF EXISTS mdst;
DROP TABLE IF EXISTS t;

CREATE TABLE dst (p UInt8, x UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS non_replicated_deduplication_window = 100;
CREATE TABLE src AS dst;
INSERT INTO dst VALUES (1, 1);
INSERT INTO dst VALUES (2, 1);
INSERT INTO src VALUES (1, 2);
ALTER TABLE dst REPLACE PARTITION 1 FROM src;
INSERT INTO dst VALUES (1, 1);
INSERT INTO dst VALUES (1, 1);
INSERT INTO dst VALUES (2, 1);
SELECT 'replace', p, x FROM dst ORDER BY p, x;

-- `remove_empty_parts = 0` keeps the empty part that covers the moved one, so the result does not depend on when it is cleaned up.
CREATE TABLE msrc (p UInt8, x UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY x SETTINGS non_replicated_deduplication_window = 100, remove_empty_parts = 0;
CREATE TABLE mdst (p UInt8, x UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO msrc VALUES (1, 1);
INSERT INTO msrc VALUES (2, 1);
ALTER TABLE msrc MOVE PARTITION 1 TO TABLE mdst;
INSERT INTO msrc VALUES (1, 1);
INSERT INTO msrc VALUES (1, 1);
INSERT INTO msrc VALUES (2, 1);
SELECT 'move', p, x FROM msrc ORDER BY p, x;

CREATE TABLE t (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t VALUES (1);
BEGIN TRANSACTION;
TRUNCATE TABLE t;
COMMIT;
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (1);
SELECT 'truncate in a transaction', count() FROM t;

DROP TABLE dst;
DROP TABLE src;
DROP TABLE msrc;
DROP TABLE mdst;
DROP TABLE t;
