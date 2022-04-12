-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01190;
CREATE DATABASE test_01190 ENGINE=Ordinary;     -- Full ATTACH requires UUID with Atomic
USE test_01190;

CREATE TABLE test_01190.table_for_dict (key UInt64, col UInt8) ENGINE = Memory;

CREATE DICTIONARY test_01190.dict (key UInt64 DEFAULT 0, col UInt8 DEFAULT 1) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'test_01190')) LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());

SHOW CREATE DICTIONARY test_01190.dict;

DETACH DICTIONARY test_01190.dict;
ATTACH TABLE test_01190.dict; -- { serverError 80 }
-- Full ATTACH syntax is not allowed for dictionaries
ATTACH DICTIONARY test_01190.dict (key UInt64 DEFAULT 0, col UInt8 DEFAULT 42) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'test_01190')) LIFETIME(MIN 1 MAX 100) LAYOUT(FLAT()); -- { clientError 62 }
ATTACH DICTIONARY test_01190.dict;
SHOW CREATE DICTIONARY test_01190.dict;

CREATE TABLE log ENGINE = Log AS SELECT 'test' AS s;
SHOW CREATE log;
DETACH TABLE log;
ATTACH DICTIONARY log; -- { serverError 80 }
ATTACH TABLE log (s String) ENGINE = Log();
SHOW CREATE log;
SELECT * FROM log;

DROP TABLE IF EXISTS mt;
CREATE TABLE mt (key Array(UInt8), s String, n UInt64, d Date MATERIALIZED '2000-01-01') ENGINE = MergeTree(d, (key, s, n), 1);
INSERT INTO mt VALUES ([1, 2], 'Hello', 2);
DETACH TABLE mt;
ATTACH TABLE mt (key Array(UInt8), s String, n UInt64, d Date MATERIALIZED '2000-01-01') ENGINE = MergeTree ORDER BY (key, s, n) PARTITION BY toYYYYMM(d); -- { serverError 342 }
ATTACH TABLE mt (key Array(UInt8), s String, n UInt64, d Date MATERIALIZED '2000-01-01') ENGINE = MergeTree(d, (key, s, n), 1);
SHOW CREATE mt;
SELECT * FROM mt;
DETACH TABLE mt;
ATTACH TABLE mt (key Array(UInt8), s String, n UInt64, d Date) ENGINE = MergeTree(d, (key, s, n), 1);   -- It works (with Ordinary database), but probably it shouldn't
SHOW CREATE mt;

CREATE MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM log;
SHOW CREATE mv;
DETACH VIEW mv;
ATTACH MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM log;
SHOW CREATE mv;
DETACH VIEW mv;
ATTACH MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM mt;  -- It works (with Ordinary database), but probably it shouldn't
SHOW CREATE mv;

SET allow_experimental_live_view = 1;
CREATE LIVE VIEW lv AS SELECT 1;
SHOW CREATE lv;
DETACH VIEW lv;
ATTACH LIVE VIEW lv AS SELECT 1;
SHOW CREATE lv;

DROP DATABASE test_01190;


