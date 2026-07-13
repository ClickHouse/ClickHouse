DROP TABLE IF EXISTS test;

CREATE TABLE test (
    s String,
    json JSON
)
ENGINE = MergeTree
ORDER BY (s)
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_rows_for_wide_part=0, min_bytes_for_wide_part=0;

INSERT INTO test SELECT 'a', '{}' FROM numbers(1);

SELECT count() FROM test WHERE s = 'a' AND json.a IS NULL;

DROP TABLE test;

