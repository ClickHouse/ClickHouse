DROP TABLE IF EXISTS tab;


# Test creation of table on top of Array(LowCardinality(FixedString)) and LowCardinality(String)
CREATE TABLE tab
(
    id UInt32,
    arr Array(LowCardinality(String)),
    arr_fixed Array(LowCardinality(FixedString(3))),
    INDEX arr_idx(arr) TYPE text(tokenizer = 'array'),
    INDEX arr_fixed_idx(arr_fixed) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, ['foo', 'bar'], ['foo', 'bar'] FROM numbers(512);
INSERT INTO tab SELECT number, ['foo', 'baz'], ['foo', 'baz'] FROM numbers(512);

-- Verify inserts succeeded
SELECT count() FROM tab WHERE has(arr, 'foo');
SELECT count() FROM tab WHERE has(arr, 'baz');

SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3));

-- Test if the index is being used
SELECT count() FROM tab WHERE has(arr, 'baz') SETTINGS log_comment = 'test_lc_array_index';


-- make sure logs are available
SYSTEM FLUSH LOGS;

-- Check index read
SELECT ProfileEvents['TextIndexReadPostings'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'test_lc_array_index'
ORDER BY event_time DESC
LIMIT 1;

-- Check that the text index on Array(LowCardinality(String)) skips granule
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE has(arr, 'baz')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

-- value does not exist in any granule
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab WHERE has(arr, 'xyz')
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Granules:%'
LIMIT 1, 2;

DROP TABLE tab;
