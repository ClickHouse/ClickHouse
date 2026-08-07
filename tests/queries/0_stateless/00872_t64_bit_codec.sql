DROP TABLE IF EXISTS t64;

CREATE TABLE t64
(
    u8 UInt8,
    t_u8 UInt8 Codec(T64('bit'), LZ4),
    u16 UInt16,
    t_u16 UInt16 Codec(T64('bit'), LZ4),
    u32 UInt32,
    t_u32 UInt32 Codec(T64('bit'), LZ4),
    u64 UInt64,
    t_u64 UInt64 Codec(T64('bit'), LZ4)
) ENGINE MergeTree() ORDER BY tuple();

-- { echoOn }

INSERT INTO t64 SELECT number AS x, x, x, x, x, x, x, x FROM numbers(1);
INSERT INTO t64 SELECT number AS x, x, x, x, x, x, x, x FROM numbers(2);
INSERT INTO t64 SELECT 42 AS x, x, x, x, x, x, x, x FROM numbers(4);

SELECT * FROM t64 ORDER BY u64;

INSERT INTO t64 SELECT number AS x, x, x, x, x, x, x, x FROM numbers(intExp2(8));
INSERT INTO t64 SELECT number AS x, x, x, x, x, x, x, x FROM numbers(intExp2(9));
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(16) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(16) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (intExp2(16) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(64);
INSERT INTO t64 SELECT (intExp2(16) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (intExp2(16) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(24) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(24) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (intExp2(24) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(128);
INSERT INTO t64 SELECT (intExp2(24) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(129);
INSERT INTO t64 SELECT (intExp2(24) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(129);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(32) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(32) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(32) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(256);
INSERT INTO t64 SELECT (intExp2(32) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(257);
INSERT INTO t64 SELECT (intExp2(32) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(257);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(40) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(40) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(40) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(512);
INSERT INTO t64 SELECT (intExp2(40) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(513);
INSERT INTO t64 SELECT (intExp2(40) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(513);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(48) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(48) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(48) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(1024);
INSERT INTO t64 SELECT (intExp2(48) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(1025);
INSERT INTO t64 SELECT (intExp2(48) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(1025);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(56) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(56) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(56) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(2048);
INSERT INTO t64 SELECT (intExp2(56) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(2049);
INSERT INTO t64 SELECT (intExp2(56) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(2049);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

INSERT INTO t64 SELECT (intExp2(63) + number * intExp2(62)) AS x, x, x, x, x, x, x, x FROM numbers(10);
SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

OPTIMIZE TABLE t64 FINAL;

SELECT * FROM t64 WHERE u8 != t_u8;
SELECT * FROM t64 WHERE u16 != t_u16;
SELECT * FROM t64 WHERE u32 != t_u32;
SELECT * FROM t64 WHERE u64 != t_u64;

DROP TABLE t64;
