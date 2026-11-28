CREATE TABLE t64_decode_narrow_signed_tails
(
    bits UInt8,
    len UInt16,
    n UInt32,
    expected_u8 UInt8 CODEC(NONE),
    expected_u64 UInt64 CODEC(NONE),
    expected_mixed Int64 CODEC(NONE),
    expected_negative Int64 CODEC(NONE),
    byte_u8 UInt8 CODEC(T64),
    byte_u64 UInt64 CODEC(T64),
    byte_mixed Int64 CODEC(T64),
    byte_negative Int64 CODEC(T64),
    bit_u8 UInt8 CODEC(T64('bit')),
    bit_u64 UInt64 CODEC(T64('bit')),
    bit_mixed Int64 CODEC(T64('bit')),
    bit_negative Int64 CODEC(T64('bit'))
)
ENGINE = MergeTree
PARTITION BY (bits, len)
ORDER BY n
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_compress_block_size = 1048576, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t64_decode_narrow_signed_tails
WITH
    bitShiftLeft(toUInt64(1), bits) - 1 AS mask,
    multiIf(n = 0, 0, n = 1, mask, bitAnd(intHash64(n), mask)) AS low,
    bitOr(0x1000000000000, low) AS high,
    toInt64(low) - toInt64(bitShiftLeft(toUInt64(1), bits - 1)) AS mixed,
    toInt64(low) - 0x10000000000 AS negative
SELECT bits, len, n,
    low, high, mixed, negative,
    low, high, mixed, negative,
    low, high, mixed, negative
FROM
(
    SELECT toUInt8(number + 1) AS bits, arrayJoin([63, 64, 65]) AS len
    FROM numbers(8)
)
ARRAY JOIN range(len) AS n;

SELECT count(), countIf(
    byte_u8 != expected_u8 OR bit_u8 != expected_u8
    OR byte_u64 != expected_u64 OR bit_u64 != expected_u64
    OR byte_mixed != expected_mixed OR bit_mixed != expected_mixed
    OR byte_negative != expected_negative OR bit_negative != expected_negative)
FROM t64_decode_narrow_signed_tails;

DROP TABLE t64_decode_narrow_signed_tails;
