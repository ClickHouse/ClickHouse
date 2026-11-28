-- A `Variant` with trillions of generated values (inside nested arrays) is refused by `max_memory_usage`
-- instead of bypassing the limit.
SELECT * FROM generateRandom('c0 Array(Array(Variant(UInt8, String)))', 1, 10, 40000000) LIMIT 1
SETTINGS max_block_size = 1, max_memory_usage = '1Gi' FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
