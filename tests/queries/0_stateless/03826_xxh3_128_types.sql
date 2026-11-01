-- Test xxh3_128 with various data types
--
-- To verify results against reference implementation (Python xxhash):
--   pip install xxhash
--
-- For strings:
--   python3 -c "import xxhash; print(xxhash.xxh3_128_hexdigest(b'ClickHouse').upper())"
--
-- For integers (uses little-endian binary representation):
--   python3 -c "import xxhash, struct; print(xxhash.xxh3_128_hexdigest(struct.pack('<B', 42)).upper())"
--   python3 -c "import xxhash, struct; print(xxhash.xxh3_128_hexdigest(struct.pack('<I', 100000)).upper())"
--
-- For floats (uses little-endian IEEE 754 binary representation):
--   python3 -c "import xxhash, struct; print(xxhash.xxh3_128_hexdigest(struct.pack('<f', 3.14159)).upper())"
--   python3 -c "import xxhash, struct; print(xxhash.xxh3_128_hexdigest(struct.pack('<d', 2.718281828459045)).upper())"
--
-- Note: Complex types (arrays, tuples) use ClickHouse's internal representation
--       and may not directly match raw binary xxhash due to formatting/padding.

-- Strings
SELECT hex(xxh3_128('ClickHouse'));
SELECT hex(xxh3_128(''));
SELECT hex(xxh3_128('test'));

-- Integers
SELECT hex(xxh3_128(toUInt8(42)));
SELECT hex(xxh3_128(toUInt32(100000)));
SELECT hex(xxh3_128(toInt8(-42)));
SELECT hex(xxh3_128(toInt64(-9223372036854775807)));

-- Floats
SELECT hex(xxh3_128(toFloat32(3.14159)));
SELECT hex(xxh3_128(toFloat64(2.718281828459045)));

-- Zero values
SELECT hex(xxh3_128(toUInt32(0)));

-- Arrays
SELECT hex(xxh3_128([1, 2, 3]));
SELECT hex(xxh3_128(['a', 'b', 'c']));

-- Tuples
SELECT hex(xxh3_128((1, 'hello')));
SELECT hex(xxh3_128((42, 3.14, 'test')));

-- Multiple arguments
SELECT hex(xxh3_128('hello', 'world'));
SELECT hex(xxh3_128(1, 2, 3));
SELECT hex(xxh3_128('foo', 42, 3.14));
