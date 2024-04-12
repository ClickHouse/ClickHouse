SET allow_drop_detached_table=1;

DROP DATABASE IF EXISTS db_03093_atomic SYNC;
CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=MergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=ReplacingMergeTree() ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=SummingMergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64) ENGINE=AggregatingMergeTree ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64, sign Int8) ENGINE=CollapsingMergeTree(sign) ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number, 1 FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table PERMANENTLY;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;

CREATE DATABASE db_03093_atomic Engine=Atomic;
CREATE TABLE db_03093_atomic.test_table  (number UInt64, sign Int8, version Int32) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY number;
INSERT INTO db_03093_atomic.test_table SELECT number, 1, 1 FROM system.numbers LIMIT 6;
DETACH TABLE db_03093_atomic.test_table;
DROP TABLE db_03093_atomic.test_table SYNC;
DROP DATABASE db_03093_atomic SYNC;
