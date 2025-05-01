DROP TABLE IF EXISTS prewhere_int128;
DROP TABLE IF EXISTS prewhere_int256;
DROP TABLE IF EXISTS prewhere_uint128;
DROP TABLE IF EXISTS prewhere_uint256;

CREATE TABLE prewhere_int128 (a Int128) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_int128 VALUES (1);
SELECT a FROM prewhere_int128 PREWHERE a; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
DROP TABLE prewhere_int128;

CREATE TABLE prewhere_int256 (a Int256) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_int256 VALUES (1);
SELECT a FROM prewhere_int256 PREWHERE a; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
DROP TABLE prewhere_int256;

CREATE TABLE prewhere_uint128 (a UInt128) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_uint128 VALUES (1);
SELECT a FROM prewhere_uint128 PREWHERE a; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
DROP TABLE prewhere_uint128;

CREATE TABLE prewhere_uint256 (a UInt256) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_uint256 VALUES (1);
SELECT a FROM prewhere_uint256 PREWHERE a; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
DROP TABLE prewhere_uint256;
