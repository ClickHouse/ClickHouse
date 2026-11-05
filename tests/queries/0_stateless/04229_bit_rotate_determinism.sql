-- bitRotateLeft / bitRotateRight must give the same result regardless of how the
-- arguments are passed (constant, column, materialize), including at the boundaries
-- of the bit width: shift counts equal to 0, equal to the bit width, larger than the
-- bit width, or negative.

-- UInt8: bits = 8
SELECT bitRotateLeft(toUInt8(0x12), toUInt8(0))    = bitRotateLeft(toUInt8(0x12), materialize(toUInt8(0)));
SELECT bitRotateLeft(toUInt8(0x12), toUInt8(8))    = bitRotateLeft(toUInt8(0x12), materialize(toUInt8(8)));
SELECT bitRotateLeft(toUInt8(0x12), toUInt8(8))    = toUInt8(0x12);
SELECT bitRotateLeft(toUInt8(0x12), toUInt8(255))  = bitRotateLeft(toUInt8(0x12), materialize(toUInt8(255)));
SELECT bitRotateLeft(toUInt8(0x12), toUInt8(255))  = bitRotateLeft(toUInt8(0x12), toUInt8(7));

-- UInt16: bits = 16
SELECT bitRotateLeft(toUInt16(0x1234), toUInt16(0))    = bitRotateLeft(toUInt16(0x1234), materialize(toUInt16(0)));
SELECT bitRotateLeft(toUInt16(0x1234), toUInt16(16))   = bitRotateLeft(toUInt16(0x1234), materialize(toUInt16(16)));
SELECT bitRotateLeft(toUInt16(0x1234), toUInt16(16))   = toUInt16(0x1234);
SELECT bitRotateLeft(toUInt16(0x1234), toUInt16(257))  = bitRotateLeft(toUInt16(0x1234), toUInt16(1));

-- UInt32: bits = 32
SELECT bitRotateLeft(toUInt32(0x12345678), toUInt32(0))   = bitRotateLeft(toUInt32(0x12345678), materialize(toUInt32(0)));
SELECT bitRotateLeft(toUInt32(0x12345678), toUInt32(32))  = bitRotateLeft(toUInt32(0x12345678), materialize(toUInt32(32)));
SELECT bitRotateLeft(toUInt32(0x12345678), toUInt32(32))  = toUInt32(0x12345678);
SELECT bitRotateLeft(toUInt32(0x12345678), toUInt32(64))  = toUInt32(0x12345678);
SELECT bitRotateLeft(toUInt32(0x12345678), toUInt32(65))  = bitRotateLeft(toUInt32(0x12345678), toUInt32(1));

-- UInt64: bits = 64
SELECT bitRotateLeft(toUInt64(0xAABBCCDDEEFF0011), toUInt64(0))    = toUInt64(0xAABBCCDDEEFF0011);
SELECT bitRotateLeft(toUInt64(0xAABBCCDDEEFF0011), toUInt64(64))   = toUInt64(0xAABBCCDDEEFF0011);
SELECT bitRotateLeft(toUInt64(0xAABBCCDDEEFF0011), toUInt64(64))   = bitRotateLeft(toUInt64(0xAABBCCDDEEFF0011), materialize(toUInt64(64)));
SELECT bitRotateLeft(toUInt64(0xAABBCCDDEEFF0011), toUInt64(128))  = toUInt64(0xAABBCCDDEEFF0011);

-- bitRotateRight: mirror of the above for representative widths.
SELECT bitRotateRight(toUInt32(0x12345678), toUInt32(0))   = toUInt32(0x12345678);
SELECT bitRotateRight(toUInt32(0x12345678), toUInt32(32))  = toUInt32(0x12345678);
SELECT bitRotateRight(toUInt32(0x12345678), toUInt32(32))  = bitRotateRight(toUInt32(0x12345678), materialize(toUInt32(32)));
SELECT bitRotateRight(toUInt32(0x12345678), toUInt32(64))  = toUInt32(0x12345678);

-- Round trip: rotate left then right by the same count, including boundary.
SELECT bitRotateRight(bitRotateLeft(toUInt32(0x12345678), toUInt32(5)), toUInt32(5)) = toUInt32(0x12345678);
SELECT bitRotateRight(bitRotateLeft(toUInt32(0x12345678), toUInt32(32)), toUInt32(32)) = toUInt32(0x12345678);
SELECT bitRotateRight(bitRotateLeft(toUInt32(0x12345678), toUInt32(99)), toUInt32(99)) = toUInt32(0x12345678);
