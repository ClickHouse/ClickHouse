-- `HASHED_ARRAY` stores its keys in a hash map (`key -> index`). Its contribution to
-- `system.dictionaries.bytes_allocated` must be the buffer size in bytes, not the element count:
-- adding the count under-reported the dictionary several-fold, which made `bytes_allocated`
-- unusable for capacity planning and hid real memory on hosts with many loaded dictionaries.

DROP DICTIONARY IF EXISTS dict_05136;
DROP TABLE IF EXISTS data_05136;

CREATE TABLE data_05136 (key UInt64, value UInt64) ENGINE = Memory
AS SELECT number, number FROM numbers(1000000);

CREATE DICTIONARY dict_05136 (key UInt64, value UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE data_05136))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

SYSTEM RELOAD DICTIONARY dict_05136;

-- A `HashMap<UInt64, size_t>` cell is 16 bytes and the capacity is rounded up to a power of two at a
-- 0.5 max fill factor, so the key container alone is ~32 bytes per element; the `UInt64` attribute
-- array adds ~8. Reporting fewer than 24 bytes per element means the key container was counted in
-- elements rather than bytes (it was ~9.4 bytes per element before the fix).
SELECT bytes_allocated / element_count >= 24
FROM system.dictionaries
WHERE database = currentDatabase() AND name = 'dict_05136';

DROP DICTIONARY dict_05136;
DROP TABLE data_05136;
