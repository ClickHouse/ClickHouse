-- Test xxh3_128 hash function (128-bit XXH3 variant)
--
-- To verify results against reference implementation (Python xxhash):
--   pip install xxhash
--   python3 -c "import xxhash; print(xxhash.xxh3_128_hexdigest(b'ClickHouse').upper())"
--
-- Expected: 14C27B7BEF95D36FECF5520CA2DAF030

SELECT hex(xxh3_128('ClickHouse'));
SELECT hex(xxh3_128(''));
SELECT hex(xxh3_128('test'));
