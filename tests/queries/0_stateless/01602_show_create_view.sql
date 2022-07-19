-- Tags: no-parallel

DROP DATABASE IF EXISTS test_1602;

CREATE DATABASE test_1602;

CREATE TABLE test_1602.tbl (`EventDate` DateTime, `CounterID` UInt32, `UserID` UInt32) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SETTINGS index_granularity = 8192;

CREATE VIEW test_1602.v AS SELECT * FROM test_1602.tbl; 

CREATE VIEW test_1602.DATABASE AS SELECT * FROM test_1602.tbl; 

CREATE VIEW test_1602.DICTIONARY AS SELECT * FROM test_1602.tbl; 

CREATE VIEW test_1602.TABLE AS SELECT * FROM test_1602.tbl; 

CREATE MATERIALIZED VIEW test_1602.vv (`EventDate` DateTime, `CounterID` UInt32, `UserID` UInt32) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SETTINGS index_granularity = 8192 AS SELECT * FROM test_1602.tbl;

CREATE VIEW test_1602.VIEW AS SELECT * FROM test_1602.tbl; 

SET allow_experimental_live_view=1;

CREATE LIVE VIEW test_1602.vvv AS SELECT * FROM test_1602.tbl;

SHOW CREATE VIEW test_1602.v;

SHOW CREATE VIEW test_1602.vv;

SHOW CREATE VIEW test_1602.vvv;

SHOW CREATE VIEW test_1602.not_exist_view; -- { serverError 390 }

SHOW CREATE VIEW test_1602.tbl; -- { serverError 36 }

SHOW CREATE TEMPORARY VIEW; -- { serverError 60 }

SHOW CREATE VIEW; -- { clientError 62 }

SHOW CREATE DATABASE; -- { clientError 62 }

SHOW CREATE DICTIONARY; -- { clientError 62 }

SHOW CREATE TABLE; -- { clientError 62 }

SHOW CREATE test_1602.VIEW;

SHOW CREATE test_1602.DATABASE;

SHOW CREATE test_1602.DICTIONARY;

SHOW CREATE test_1602.TABLE;

DROP DATABASE IF EXISTS test_1602;
