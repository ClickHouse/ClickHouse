DROP TABLE IF EXISTS codecs;

CREATE TABLE codecs
(
    a UInt8 CODEC(LZ4),
    b UInt16 CODEC(ZSTD),
    c Float32 CODEC(Gorilla),
    d UInt8 CODEC(Delta, LZ4),
    e Float64 CODEC(Gorilla, ZSTD),
    f UInt32 CODEC(Delta, Delta, T64),
    g DateTime CODEC(DoubleDelta),
    h DateTime64 CODEC(DoubleDelta, LZ4),
    i String CODEC(NONE)
) ENGINE = MergeTree ORDER BY tuple();

DROP TABLE codecs;

-- test what should not work

CREATE TABLE codecs (a UInt8 CODEC(NONE, NONE)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(NONE, LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(LZ4, NONE)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(LZ4, LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(LZ4, ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(LZ4, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a UInt8 CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a FixedString(2) CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a Decimal(15,5) CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a Float64 CODEC(Delta, Gorilla)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecs (a Float32 CODEC(DoubleDelta, FPC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- test that sanity check is not performed in ATTACH query

DROP TABLE IF EXISTS codecs1;
DROP TABLE IF EXISTS codecs2;
DROP TABLE IF EXISTS codecs3;
DROP TABLE IF EXISTS codecs4;
DROP TABLE IF EXISTS codecs5;
DROP TABLE IF EXISTS codecs6;
DROP TABLE IF EXISTS codecs7;
DROP TABLE IF EXISTS codecs8;
DROP TABLE IF EXISTS codecs9;
DROP TABLE IF EXISTS codecs10;
DROP TABLE IF EXISTS codecs11;

SET allow_suspicious_codecs = 1;

CREATE TABLE codecs1 (a UInt8 CODEC(NONE, NONE)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs2 (a UInt8 CODEC(NONE, LZ4)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs3 (a UInt8 CODEC(LZ4, NONE)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs4 (a UInt8 CODEC(LZ4, LZ4)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs5 (a UInt8 CODEC(LZ4, ZSTD)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs6 (a UInt8 CODEC(Delta)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs7 (a UInt8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs8 (a UInt8 CODEC(LZ4, Delta)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs9 (a UInt8 CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs10 (a FixedString(2) CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE codecs11 (a Decimal(15,5) CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple();

SET allow_suspicious_codecs = 0;

SHOW CREATE TABLE codecs1;
SHOW CREATE TABLE codecs2;
SHOW CREATE TABLE codecs3;
SHOW CREATE TABLE codecs4;
SHOW CREATE TABLE codecs5;
SHOW CREATE TABLE codecs6;
SHOW CREATE TABLE codecs7;
SHOW CREATE TABLE codecs8;
SHOW CREATE TABLE codecs9;
SHOW CREATE TABLE codecs10;
SHOW CREATE TABLE codecs11;

DETACH TABLE codecs1;
DETACH TABLE codecs2;
DETACH TABLE codecs3;
DETACH TABLE codecs4;
DETACH TABLE codecs5;
DETACH TABLE codecs6;
DETACH TABLE codecs7;
DETACH TABLE codecs8;
DETACH TABLE codecs9;
DETACH TABLE codecs10;
DETACH TABLE codecs11;

ATTACH TABLE codecs1;
ATTACH TABLE codecs2;
ATTACH TABLE codecs3;
ATTACH TABLE codecs4;
ATTACH TABLE codecs5;
ATTACH TABLE codecs6;
ATTACH TABLE codecs7;
ATTACH TABLE codecs8;
ATTACH TABLE codecs9;
ATTACH TABLE codecs10;
ATTACH TABLE codecs11;

SHOW CREATE TABLE codecs1;
SHOW CREATE TABLE codecs2;
SHOW CREATE TABLE codecs3;
SHOW CREATE TABLE codecs4;
SHOW CREATE TABLE codecs5;
SHOW CREATE TABLE codecs6;
SHOW CREATE TABLE codecs7;
SHOW CREATE TABLE codecs8;
SHOW CREATE TABLE codecs9;
SHOW CREATE TABLE codecs10;
SHOW CREATE TABLE codecs11;

SELECT * FROM codecs1;
SELECT * FROM codecs2;
SELECT * FROM codecs3;
SELECT * FROM codecs4;
SELECT * FROM codecs5;
SELECT * FROM codecs6;
SELECT * FROM codecs7;
SELECT * FROM codecs8;
SELECT * FROM codecs9;
SELECT * FROM codecs10;
SELECT * FROM codecs11;

DROP TABLE codecs1;
DROP TABLE codecs2;
DROP TABLE codecs3;
DROP TABLE codecs4;
DROP TABLE codecs5;
DROP TABLE codecs6;
DROP TABLE codecs7;
DROP TABLE codecs8;
DROP TABLE codecs9;
DROP TABLE codecs10;
DROP TABLE codecs11;
