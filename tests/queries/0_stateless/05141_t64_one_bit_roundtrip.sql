CREATE TABLE t64_one_bit
(
    n UInt32,
    flag UInt8 CODEC(NONE),
    u8 UInt8 DEFAULT flag + 0x80 CODEC(T64),
    u16 UInt16 DEFAULT flag + 0x8000 CODEC(T64),
    u32 UInt32 DEFAULT flag + 0x80000000 CODEC(T64),
    u64 UInt64 DEFAULT flag + 0x8000000000000000 CODEC(T64),
    u64_bit_variant UInt64 DEFAULT flag + 0x8000000000000000 CODEC(T64('bit')),
    i8 Int8 DEFAULT flag - 1 CODEC(T64),
    i16 Int16 DEFAULT flag - 1 CODEC(T64),
    i32 Int32 DEFAULT flag - 1 CODEC(T64),
    i64 Int64 DEFAULT flag - 1 CODEC(T64)
)
ENGINE = MergeTree
ORDER BY n
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_compress_block_size = 1048576, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- Uncompressed twin of the table above.
CREATE TABLE t64_one_bit_plain AS t64_one_bit ENGINE = Memory;

-- Fixed pseudo-random flags.
INSERT INTO t64_one_bit_plain (n, flag) SELECT number, intHash64(number) % 2 FROM numbers(100003);
INSERT INTO t64_one_bit SELECT * FROM t64_one_bit_plain;

SELECT count() FROM t64_one_bit;
SELECT count() FROM (SELECT * FROM t64_one_bit EXCEPT SELECT * FROM t64_one_bit_plain);

DROP TABLE t64_one_bit;
DROP TABLE t64_one_bit_plain;
