-- Tags: long

-- We use temporary files that uses NativeReader, with a block slightly bigger
-- then power of two, previously it rounds the allocation up to power of 2,
-- which leads to excessive memory usage.

-- This will create a temporary buffer of two columns for the total of 5m rows
SELECT number FROM numbers(5e6) ORDER BY number * 1234567890123456789 LIMIT 4999980, 20
SETTINGS
    max_threads=1,
    max_memory_usage='200Mi',
    /* 65536 rows takes buffer of 512KB, so use slightly bigger value to increase overhead */
    max_block_size=65540,
    max_bytes_before_external_sort='2Mi',
    max_bytes_ratio_before_external_sort=0
FORMAT Null
