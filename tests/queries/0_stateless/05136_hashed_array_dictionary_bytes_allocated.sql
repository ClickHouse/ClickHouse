-- `HASHED_ARRAY` accounting in `system.dictionaries.bytes_allocated` must be in bytes, not in
-- elements. Two terms were counted in elements:
--   * the key containers, which are hash maps (`key -> index`);
--   * the per-attribute null masks, which are `std::vector<bool>` and therefore bit-packed.

DROP DICTIONARY IF EXISTS dict_05136;
DROP DICTIONARY IF EXISTS dict_05136_nullable;
DROP TABLE IF EXISTS data_05136;

CREATE TABLE data_05136 (key UInt64, value UInt64, value_nullable Nullable(UInt64)) ENGINE = Memory
AS SELECT number, number, if(number % 2 = 0, number, NULL) FROM numbers(1000000);

CREATE DICTIONARY dict_05136 (key UInt64, value UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE data_05136))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

CREATE DICTIONARY dict_05136_nullable (key UInt64, value_nullable Nullable(UInt64))
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE data_05136))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

SYSTEM RELOAD DICTIONARY dict_05136;
SYSTEM RELOAD DICTIONARY dict_05136_nullable;

-- A `HashMap<UInt64, size_t>` cell is 16 bytes and the capacity is rounded up to a power of two at a
-- 0.5 max fill factor, so the key container alone is ~32 bytes per element; the `UInt64` attribute
-- array adds ~8. Reporting fewer than 24 bytes per element means the key container was counted in
-- elements rather than bytes (it reported ~9.4 bytes per element before the fix).
SELECT bytes_allocated / element_count >= 24
FROM system.dictionaries
WHERE database = currentDatabase() AND name = 'dict_05136';

-- The difference between the nullable and the non-nullable dictionary is the null mask, and nothing
-- else: both hold one `UInt64` attribute array of the same size. The mask is bit-packed, so it must
-- cost about `element_count / 8` bytes.
--
-- Bound it on both sides. The upper bound fails on the original accounting, which added the flag
-- count and made this term `element_count` (1,000,000 instead of 131,072 for this data). The lower
-- bound fails if the term is ever dropped or undercounted, which a one-sided check would accept.
-- `std::vector<bool>` allocates at least `size` bits and the accounting uses `capacity()`, which is
-- never below `size`, so a correct implementation cannot report less than `element_count / 8`.
WITH
    (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_05136_nullable') AS nullable_bytes,
    (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_05136') AS plain_bytes,
    (SELECT element_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_05136') AS elements
SELECT (nullable_bytes - plain_bytes) BETWEEN (elements / 8) AND (elements / 2);

DROP DICTIONARY dict_05136;
DROP DICTIONARY dict_05136_nullable;
DROP TABLE data_05136;
