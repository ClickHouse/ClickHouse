-- ByteReverse codec: a preprocessor that reverses the bytes of every compressed block.
-- Round trip across many odd-sized compressed blocks (blocks end in the middle of a row), merges, mutations, and error handling.

DROP TABLE IF EXISTS t_byte_reverse_src;
DROP TABLE IF EXISTS t_byte_reverse;

-- The codec is experimental: without the setting it is rejected.
SET enable_bytereverse_codec = 0;
CREATE TABLE t_byte_reverse_bad (s String CODEC(ByteReverse, ZSTD(3))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SET enable_bytereverse_codec = 1;

-- It transforms the data but does not compress it, so a general purpose codec is required, and it must not follow one.
CREATE TABLE t_byte_reverse_bad (s String CODEC(ByteReverse)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_byte_reverse_bad (s String CODEC(ZSTD(3), ByteReverse)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- It takes no parameters.
CREATE TABLE t_byte_reverse_bad (s String CODEC(ByteReverse(1), ZSTD(3))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- Log-like rows of different lengths, including empty strings and rows with a long variable part.
CREATE TABLE t_byte_reverse_src (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_byte_reverse_src
SELECT number,
       if(number % 97 = 0, '',
          concat('2026-10-', lpad(toString(1 + number % 28), 2, '0'), ' 12:', lpad(toString(number % 60), 2, '0'), ':', lpad(toString((number * 7) % 60), 2, '0'),
                 ' INFO [worker-', toString(number % 16), '] org.example.Service: handled request id=', toString(cityHash64(number) % 100000),
                 ' status=', if(number % 5 = 0, 'ERROR', 'OK'), repeat('x', number % 41)))
FROM numbers(60000);

-- A small maximum block size makes the compressed blocks odd-sized, so that many of them end in the middle of a row.
CREATE TABLE t_byte_reverse
(
    id UInt64,
    s String CODEC(ByteReverse, ZSTD(3)),
    s_lz4 String CODEC(ByteReverse, LZ4),
    s_nested Array(String) CODEC(ByteReverse, ZSTD(1)),
    s_null Nullable(String) CODEC(ByteReverse, ZSTD(3)),
    fixed FixedString(7) CODEC(ByteReverse, ZSTD(3)),
    n UInt32 CODEC(ByteReverse, ZSTD(3)),
    ref String CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000, min_compress_block_size = 1000, max_compress_block_size = 4099;

INSERT INTO t_byte_reverse
SELECT id, s, s, [s, '', reverse(s)], if(id % 3 = 0, NULL, s), substring(concat(s, 'abcdefg'), 1, 7), toUInt32(cityHash64(id) % 4294967291), s
FROM t_byte_reverse_src;

SELECT 'rows', count() FROM t_byte_reverse;

-- Every column must decompress back to exactly the source values (0 mismatching rows).
SELECT 's', countIf(s != ref) FROM t_byte_reverse;
SELECT 's_lz4', countIf(s_lz4 != ref) FROM t_byte_reverse;
SELECT 's_nested', countIf(s_nested != [ref, '', reverse(ref)]) FROM t_byte_reverse;
SELECT 's_null', countIf(s_null IS NULL AND id % 3 != 0), countIf(s_null IS NOT NULL AND s_null != ref) FROM t_byte_reverse;
SELECT 'fixed', countIf(fixed != substring(concat(ref, 'abcdefg'), 1, 7)) FROM t_byte_reverse;
SELECT 'n', countIf(n != toUInt32(cityHash64(id) % 4294967291)) FROM t_byte_reverse;

-- Checksums over the whole column, and over single rows read through the primary index (reads in the middle of a block).
SELECT 'hash', sum(cityHash64(s)) = (SELECT sum(cityHash64(s)) FROM t_byte_reverse_src) FROM t_byte_reverse;
SELECT 'point reads', countIf(s != ref) FROM t_byte_reverse WHERE id IN (0, 1, 96, 97, 4095, 4096, 4097, 29999, 30000, 59999);
SELECT 'ranges', countIf(s != ref) FROM t_byte_reverse WHERE id % 1000 BETWEEN 3 AND 9;

SELECT name, compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_byte_reverse' ORDER BY name;

-- Merges and mutations rewrite the column through the same codec chain.
INSERT INTO t_byte_reverse
SELECT id + 100000, s, s, [s, '', reverse(s)], if(id % 3 = 0, NULL, s), substring(concat(s, 'abcdefg'), 1, 7), 1, s
FROM t_byte_reverse_src WHERE id < 20000;
OPTIMIZE TABLE t_byte_reverse FINAL;
SELECT 'after merge', count(), countIf(s != ref), countIf(s_lz4 != ref) FROM t_byte_reverse;

ALTER TABLE t_byte_reverse MODIFY COLUMN ref String CODEC(ByteReverse, ZSTD(3)) SETTINGS mutations_sync = 2;
SELECT 'after codec change', count(), countIf(s != ref), countIf(s_lz4 != ref) FROM t_byte_reverse;
ALTER TABLE t_byte_reverse MODIFY COLUMN s_lz4 String CODEC(ZSTD(1)) SETTINGS mutations_sync = 2;
SELECT 'after codec removal', count(), countIf(s != ref), countIf(s_lz4 != ref) FROM t_byte_reverse;

-- A table that uses the codec can be attached again, and the data stays readable.
DETACH TABLE t_byte_reverse;
ATTACH TABLE t_byte_reverse;
SELECT 'after attach', count(), countIf(s != ref) FROM t_byte_reverse;

SELECT 'columns with the codec', countIf(compression_codec LIKE '%ByteReverse%') FROM system.columns WHERE database = currentDatabase() AND table = 't_byte_reverse';

DROP TABLE t_byte_reverse;
DROP TABLE t_byte_reverse_src;
