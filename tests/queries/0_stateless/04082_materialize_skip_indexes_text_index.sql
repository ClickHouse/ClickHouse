-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101666
-- materialize_skip_indexes_on_merge=false must suppress text (full-text) indexes during merge,
-- not only minmax/set/bloom_filter etc. Text indexes use a separate container
-- (text_indexes_to_merge) which was previously not cleared by the setting.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_text_idx;

CREATE TABLE t_text_idx
(
    id UInt64,
    s String,
    INDEX idx_text s TYPE text(tokenizer='splitByNonAlpha') GRANULARITY 1,
    INDEX idx_minmax id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, materialize_skip_indexes_on_merge = 0;

INSERT INTO t_text_idx SELECT number, 'hello world' FROM numbers(20);
INSERT INTO t_text_idx SELECT number + 20, 'hello world' FROM numbers(20);

OPTIMIZE TABLE t_text_idx FINAL;

-- Both indexes should have no data after merge with materialize_skip_indexes_on_merge=0
SELECT 'After merge with materialize_skip_indexes_on_merge=0';
SELECT name, data_compressed_bytes > 0 AS has_data
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_text_idx'
ORDER BY name;

-- Explicit MATERIALIZE INDEX should still rebuild them
ALTER TABLE t_text_idx MATERIALIZE INDEX idx_text;
ALTER TABLE t_text_idx MATERIALIZE INDEX idx_minmax;

SELECT 'After explicit MATERIALIZE INDEX';
SELECT name, data_compressed_bytes > 0 AS has_data
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_text_idx'
ORDER BY name;

-- Reset setting and verify indexes are built during merge again
ALTER TABLE t_text_idx MODIFY SETTING materialize_skip_indexes_on_merge = 1;

TRUNCATE TABLE t_text_idx;
INSERT INTO t_text_idx SELECT number, 'hello world' FROM numbers(20);
INSERT INTO t_text_idx SELECT number + 20, 'hello world' FROM numbers(20);

OPTIMIZE TABLE t_text_idx FINAL;

SELECT 'After merge with materialize_skip_indexes_on_merge=1';
SELECT name, data_compressed_bytes > 0 AS has_data
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_text_idx'
ORDER BY name;

DROP TABLE t_text_idx;

-- Also test exclude_materialize_skip_indexes_on_merge with text indexes
DROP TABLE IF EXISTS t_text_exclude;

CREATE TABLE t_text_exclude
(
    id UInt64,
    s String,
    INDEX idx_text s TYPE text(tokenizer='splitByNonAlpha') GRANULARITY 1,
    INDEX idx_minmax id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, exclude_materialize_skip_indexes_on_merge = 'idx_text';

INSERT INTO t_text_exclude SELECT number, 'hello world' FROM numbers(20);
INSERT INTO t_text_exclude SELECT number + 20, 'hello world' FROM numbers(20);

OPTIMIZE TABLE t_text_exclude FINAL;

-- Only idx_text should be excluded; idx_minmax should have data
SELECT 'After merge with exclude idx_text';
SELECT name, data_compressed_bytes > 0 AS has_data
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_text_exclude'
ORDER BY name;

DROP TABLE t_text_exclude;
