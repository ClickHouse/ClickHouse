DROP TABLE IF EXISTS test_04061;

CREATE TABLE test_04061
(
    id UInt64,
    arr Array(LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1,
    index_granularity = 1,
    auto_statistics_types = '';

INSERT INTO test_04061
SELECT
    number,
    CAST(emptyArrayString(), 'Array(LowCardinality(String))')
FROM numbers(4);

INSERT INTO test_04061
SELECT
    number + 4,
    CAST(emptyArrayString(), 'Array(LowCardinality(String))')
FROM numbers(4);

OPTIMIZE TABLE test_04061 FINAL SETTINGS mutations_sync = 1;

-- Empty arrays produce a nested `LowCardinality` stream with only `keys_version`.
SELECT arr
FROM test_04061
ORDER BY id
FORMAT Null;

SELECT count()
FROM test_04061;

DROP TABLE IF EXISTS test_04061;
