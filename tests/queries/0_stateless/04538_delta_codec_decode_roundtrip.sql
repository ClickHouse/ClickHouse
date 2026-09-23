-- Roundtrip check for Delta codec decoding: every width (1/2/4/8 bytes) over random
-- full-range data, with part sizes around the 16-byte SIMD block boundary as well as
-- large parts, so both the vectorized kernel and the scalar tail are exercised.

DROP TABLE IF EXISTS delta_decode_roundtrip;

CREATE TABLE delta_decode_roundtrip
(
    key UInt64,
    u8 UInt8 CODEC(Delta, LZ4),
    u8_ref UInt8,
    u16 UInt16 CODEC(Delta, LZ4),
    u16_ref UInt16,
    u32 UInt32 CODEC(Delta, LZ4),
    u32_ref UInt32,
    u64 UInt64 CODEC(Delta, LZ4),
    u64_ref UInt64
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO delta_decode_roundtrip
SELECT
    number,
    toUInt8(intHash64(number)), toUInt8(intHash64(number)),
    toUInt16(intHash64(number)), toUInt16(intHash64(number)),
    toUInt32(intHash64(number)), toUInt32(intHash64(number)),
    intHash64(number), intHash64(number)
FROM numbers(65553);

INSERT INTO delta_decode_roundtrip
SELECT
    2000000 + number,
    toUInt8(intHash64(number)), toUInt8(intHash64(number)),
    toUInt16(intHash64(number)), toUInt16(intHash64(number)),
    toUInt32(intHash64(number)), toUInt32(intHash64(number)),
    intHash64(number), intHash64(number)
FROM numbers(1);

INSERT INTO delta_decode_roundtrip
SELECT
    3000000 + number,
    toUInt8(intHash64(number)), toUInt8(intHash64(number)),
    toUInt16(intHash64(number)), toUInt16(intHash64(number)),
    toUInt32(intHash64(number)), toUInt32(intHash64(number)),
    intHash64(number), intHash64(number)
FROM numbers(15);

INSERT INTO delta_decode_roundtrip
SELECT
    4000000 + number,
    toUInt8(intHash64(number)), toUInt8(intHash64(number)),
    toUInt16(intHash64(number)), toUInt16(intHash64(number)),
    toUInt32(intHash64(number)), toUInt32(intHash64(number)),
    intHash64(number), intHash64(number)
FROM numbers(16);

INSERT INTO delta_decode_roundtrip
SELECT
    5000000 + number,
    toUInt8(intHash64(number)), toUInt8(intHash64(number)),
    toUInt16(intHash64(number)), toUInt16(intHash64(number)),
    toUInt32(intHash64(number)), toUInt32(intHash64(number)),
    intHash64(number), intHash64(number)
FROM numbers(17);

INSERT INTO delta_decode_roundtrip
SELECT
    6000000 + number,
    toUInt8(intHash64(number)), toUInt8(intHash64(number)),
    toUInt16(intHash64(number)), toUInt16(intHash64(number)),
    toUInt32(intHash64(number)), toUInt32(intHash64(number)),
    intHash64(number), intHash64(number)
FROM numbers(33);

SELECT count() FROM delta_decode_roundtrip;

SELECT count()
FROM delta_decode_roundtrip
WHERE u8 != u8_ref OR u16 != u16_ref OR u32 != u32_ref OR u64 != u64_ref;

OPTIMIZE TABLE delta_decode_roundtrip FINAL;

SELECT count()
FROM delta_decode_roundtrip
WHERE u8 != u8_ref OR u16 != u16_ref OR u32 != u32_ref OR u64 != u64_ref;

DROP TABLE delta_decode_roundtrip;
