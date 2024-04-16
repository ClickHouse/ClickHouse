-- Tags: no-parallel

SET allow_drop_detached_table=1;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO test_table_03093 SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_table_03093 SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO test_table_03093 SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_table_03093 SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093 PERMANENTLY;
DROP TABLE test_table_03093 SYNC;

CREATE TABLE test_table_03093  (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO test_table_03093 SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE test_table_03093;
DROP TABLE test_table_03093 SYNC;