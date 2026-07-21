-- ZXC compression codec: roundtrip correctness across all levels, plus error handling.

DROP TABLE IF EXISTS t_zxc_src;
DROP TABLE IF EXISTS t_zxc;

CREATE TABLE t_zxc_src (id UInt64, s String, v UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_zxc_src
    SELECT number,
           concat('r', toString(number % 9973), repeat('z', number % 37)),
           toUInt32((number * 2654435761) % 1000000)
    FROM numbers(50000);

-- One column per level (1..7), a default-level column, and a numeric column.
CREATE TABLE t_zxc
(
    id UInt64,
    s1 String CODEC(ZXC(1)),
    s2 String CODEC(ZXC(2)),
    s3 String CODEC(ZXC(3)),
    s4 String CODEC(ZXC(4)),
    s5 String CODEC(ZXC(5)),
    s6 String CODEC(ZXC(6)),
    s7 String CODEC(ZXC(7)),
    sd String CODEC(ZXC),
    v  UInt32 CODEC(ZXC(5))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_zxc SELECT id, s, s, s, s, s, s, s, s, v FROM t_zxc_src;

SELECT 'rows', count() FROM t_zxc;

-- Each ZXC column must decompress back to exactly the source values (1 = identical).
SELECT 'l1', (SELECT sum(cityHash64(s1)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'l2', (SELECT sum(cityHash64(s2)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'l3', (SELECT sum(cityHash64(s3)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'l4', (SELECT sum(cityHash64(s4)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'l5', (SELECT sum(cityHash64(s5)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'l6', (SELECT sum(cityHash64(s6)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'l7', (SELECT sum(cityHash64(s7)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'ld', (SELECT sum(cityHash64(sd)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);
SELECT 'v',  (SELECT sum(v) FROM t_zxc) = (SELECT sum(v) FROM t_zxc_src);

-- Codec is recorded on all 9 ZXC columns.
SELECT 'codec_present', count()
FROM system.columns
WHERE database = currentDatabase() AND table = 't_zxc' AND compression_codec LIKE '%ZXC%';

-- Data persists across detach/attach (read back from disk).
DETACH TABLE t_zxc;
ATTACH TABLE t_zxc;
SELECT 'after_attach', (SELECT sum(cityHash64(sd)) FROM t_zxc) = (SELECT sum(cityHash64(s)) FROM t_zxc_src);

-- Invalid parameters are rejected.
CREATE TABLE t_zxc_bad (x UInt64 CODEC(ZXC(0)))  ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_zxc_bad (x UInt64 CODEC(ZXC(8)))  ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE t_zxc_bad (x UInt64 CODEC(ZXC(1, 2))) ENGINE = MergeTree ORDER BY x; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

DROP TABLE t_zxc;
DROP TABLE t_zxc_src;
