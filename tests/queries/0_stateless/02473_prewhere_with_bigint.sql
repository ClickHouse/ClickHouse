CREATE TABLE prewhere_int128 (a Int128) ENGINE=MergeTree ORDER BY a;
SELECT a FROM prewhere_int128 WHERE a; -- { serverError 59 }
DROP TABLE prewhere_int128;

CREATE TABLE prewhere_int256 (a Int256) ENGINE=MergeTree ORDER BY a;
SELECT a FROM prewhere_int256 WHERE a; -- { serverError 59 }
DROP TABLE prewhere_int256;

CREATE TABLE prewhere_uint128 (a UInt128) ENGINE=MergeTree ORDER BY a;
SELECT a FROM prewhere_uint128 WHERE a; -- { serverError 59 }
DROP TABLE prewhere_uint128;

CREATE TABLE prewhere_uint256 (a UInt256) ENGINE=MergeTree ORDER BY a;
SELECT a FROM prewhere_uint256 WHERE a; -- { serverError 59 }
DROP TABLE prewhere_uint256;
