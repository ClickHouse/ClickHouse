-- Tags: no-parallel-replicas, no-azure-blob-storage

-- Tests that text indexes can be created on and used with Array(LowCardinality(String)) and
-- Array(LowCardinality(FixedString)) columns.

DROP TABLE IF EXISTS tab;

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

INSERT INTO tab SELECT number, ['foo',        'all'], ['foo',        'all'] FROM numbers(512);
INSERT INTO tab SELECT number, ['bar',        'all'], ['bar',        'all'] FROM numbers(512);
INSERT INTO tab SELECT number, ['foo', 'baz', 'all'], ['foo', 'baz', 'all'] FROM numbers(512);

-- Verify inserts succeeded
SELECT count() FROM tab WHERE has(arr, 'foo');
SELECT count() FROM tab WHERE has(arr, 'baz');
SELECT count() FROM tab WHERE has(arr, 'xyz');

SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('foo', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('baz', 3));
SELECT count() FROM tab WHERE has(arr_fixed, toFixedString('xyz', 3));

-- Run a query with a log_comment and check system.query_log to confirm the text index
-- machinery was engaged (TextIndexReadPostings > 0).
SELECT count() FROM tab WHERE has(arr, 'baz') SETTINGS log_comment = 'test_lc_arr';

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['TextIndexReadPostings'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = 'test_lc_arr'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE tab;
