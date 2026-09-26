SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS test_04060;

CREATE TABLE test_04060
(
    id UInt64,
    v Variant(String, LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1,
    index_granularity = 1,
    auto_statistics_types = '';

INSERT INTO test_04060
SELECT
    number,
    CAST('str_' || toString(number), 'Variant(String, LowCardinality(String))')
FROM numbers(4);

INSERT INTO test_04060
SELECT
    number + 4,
    CAST('str_' || toString(number + 4), 'Variant(String, LowCardinality(String))')
FROM numbers(4);

OPTIMIZE TABLE test_04060 FINAL SETTINGS mutations_sync = 1;

-- The `LowCardinality(String)` branch is never used, so its `DictionaryKeys`
-- stream contains only `keys_version`. The read must not try to eagerly read
-- `num_keys` from that version-only prefix.
SELECT v
FROM test_04060
ORDER BY id
FORMAT Null;

SELECT count()
FROM test_04060;

DROP TABLE IF EXISTS test_04060;
