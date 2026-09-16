-- RANGE_HASHED dictionaries store nullable attribute masks as bit-packed `std::vector<bool>`.
-- Compare otherwise equivalent dictionaries to verify that `bytes_allocated` counts this mask in
-- bytes rather than counting one byte per row.

-- Create a source table with both non-nullable and nullable versions of the same value.
CREATE TABLE range_hashed_bytes_src
(
    id UInt64,
    range_min Date,
    range_max Date,
    value UInt64,
    value_nullable Nullable(UInt64)
)
ENGINE = Memory
AS SELECT
    number,
    toDate('2020-01-01'),
    toDate('2030-01-01'),
    number,
    if(number % 2 = 0, number, NULL)
FROM numbers(100000);

-- Create the non-nullable dictionary as the memory-accounting baseline.
CREATE DICTIONARY range_hashed_bytes
(
    id UInt64,
    range_min Date,
    range_max Date,
    value UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'range_hashed_bytes_src' DB currentDatabase()))
LAYOUT(RANGE_HASHED())
RANGE(MIN range_min MAX range_max)
LIFETIME(0);

-- Create an otherwise equivalent dictionary with a nullable attribute.
CREATE DICTIONARY range_hashed_bytes_nullable
(
    id UInt64,
    range_min Date,
    range_max Date,
    value_nullable Nullable(UInt64)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'range_hashed_bytes_src' DB currentDatabase()))
LAYOUT(RANGE_HASHED())
RANGE(MIN range_min MAX range_max)
LIFETIME(0);

-- Load both dictionaries before reading their memory statistics.
SYSTEM RELOAD DICTIONARY range_hashed_bytes;
SYSTEM RELOAD DICTIONARY range_hashed_bytes_nullable;

-- The nullable mask must use at least one bit per dictionary element.
SELECT
    (
        SELECT bytes_allocated
        FROM system.dictionaries
        WHERE database = currentDatabase() AND name = 'range_hashed_bytes_nullable'
    )
    - (
        SELECT bytes_allocated
        FROM system.dictionaries
        WHERE database = currentDatabase() AND name = 'range_hashed_bytes'
    )
    >= (
        SELECT element_count / 8
        FROM system.dictionaries
        WHERE database = currentDatabase() AND name = 'range_hashed_bytes'
    );

-- The nullable mask must not be accounted for as one full byte per element.
SELECT
    (
        SELECT bytes_allocated
        FROM system.dictionaries
        WHERE database = currentDatabase() AND name = 'range_hashed_bytes_nullable'
    )
    - (
        SELECT bytes_allocated
        FROM system.dictionaries
        WHERE database = currentDatabase() AND name = 'range_hashed_bytes'
    )
    < (
        SELECT element_count / 2
        FROM system.dictionaries
        WHERE database = currentDatabase() AND name = 'range_hashed_bytes'
    );

-- Remove the dictionaries and source table created by this test.
DROP DICTIONARY range_hashed_bytes;
DROP DICTIONARY range_hashed_bytes_nullable;
DROP TABLE range_hashed_bytes_src;
