-- { echo }

-- A skip index over mapKeys(m) or mapValues(m) answers like a full scan when the map keys or values are
-- LowCardinality: after INSERT, after a mutation that rewrites the part, when the sorting key or an index of
-- another type uses the same expression, when the expression is nested in a larger one, and when a table column
-- has the name of the expression. Each probe compares the indexed count with an unindexed one and shows the
-- granules the index keeps.

DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_mut;
DROP TABLE IF EXISTS t_order;
DROP TABLE IF EXISTS t_mixed;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_named;

CREATE TABLE t_lc (id UInt64, m Map(LowCardinality(String), LowCardinality(String)),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1, INDEX iv mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;
INSERT INTO t_lc SELECT number, map(concat('k', toString(intDiv(number, 1000))), concat('v', toString(intDiv(number, 1000))), 'common', 'c')
FROM numbers(10000) SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_lc WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_lc WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';
SELECT (SELECT count() FROM t_lc WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_lc WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';

CREATE TABLE t_plain (id UInt64, m Map(String, String),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1, INDEX iv mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;
INSERT INTO t_plain SELECT * FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_plain WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_plain WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_plain WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';
SELECT (SELECT count() FROM t_plain WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_plain WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 0);

-- A compact part: the mutation rewrites the whole part, so the index must describe the new map.
CREATE TABLE t_mut (id UInt64, m Map(LowCardinality(String), LowCardinality(String)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_mut SELECT * FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
ALTER TABLE t_mut ADD INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE t_mut UPDATE m = map(concat('n', toString(intDiv(id, 1000))), 'w') WHERE id >= 5000, MATERIALIZE INDEX ik SETTINGS mutations_sync = 2;
SELECT (SELECT count() FROM t_mut WHERE has(mapKeys(m), 'n7') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_mut WHERE has(mapKeys(m), 'n7') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mut WHERE has(mapKeys(m), 'n7') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';
SELECT (SELECT count() FROM t_mut WHERE has(mapKeys(m), 'k7') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_mut WHERE has(mapKeys(m), 'k7') SETTINGS use_skip_indexes = 0);
SELECT (SELECT count() FROM t_mut WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_mut WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);

-- The sorting key and the keys index share one expression.
CREATE TABLE t_order (id UInt64, m Map(LowCardinality(String), LowCardinality(String)),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1, INDEX iv mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY mapKeys(m) SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;
INSERT INTO t_order SELECT * FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_order WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_order WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);
SELECT (SELECT count() FROM t_order WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_order WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_order WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';

-- Indexes of four types over the same expression, built on INSERT and rebuilt by a mutation of a compact part.
CREATE TABLE t_mixed (id UInt64, m Map(LowCardinality(String), LowCardinality(String)),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1, INDEX is mapKeys(m) TYPE set(100) GRANULARITY 1,
    INDEX im mapKeys(m) TYPE minmax GRANULARITY 1, INDEX it mapKeys(m) TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_mixed SELECT * FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS ignore_data_skipping_indices = 'is,im,it'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS ignore_data_skipping_indices = 'ik,im,it'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS ignore_data_skipping_indices = 'ik,is,it'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS ignore_data_skipping_indices = 'ik,is,im'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes_on_data_read = 0, ignore_data_skipping_indices = 'is,im,it') WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes_on_data_read = 0, ignore_data_skipping_indices = 'ik,im,it') WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';
ALTER TABLE t_mixed MATERIALIZE INDEX ik, MATERIALIZE INDEX is, MATERIALIZE INDEX im, MATERIALIZE INDEX it SETTINGS mutations_sync = 2;
SELECT (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k4') SETTINGS ignore_data_skipping_indices = 'is,im,it'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k4') SETTINGS ignore_data_skipping_indices = 'ik,im,it'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k4') SETTINGS ignore_data_skipping_indices = 'ik,is,it'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k4') SETTINGS ignore_data_skipping_indices = 'ik,is,im'),
       (SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k4') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mixed WHERE has(mapKeys(m), 'k4') SETTINGS use_skip_indexes_on_data_read = 0, ignore_data_skipping_indices = 'is,im,it') WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';

-- mapKeys(m) nested in a larger expression, next to an index over mapKeys(m) itself.
CREATE TABLE t_nested (id UInt64, m Map(LowCardinality(String), String),
    INDEX ic formatRowNoNewline('JSONEachRow', mapKeys(m)) TYPE bloom_filter GRANULARITY 1, INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;
INSERT INTO t_nested SELECT * FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_nested WHERE formatRowNoNewline('JSONEachRow', mapKeys(m)) = '{"mapKeys(m)":["k3","common"]}' SETTINGS use_skip_indexes = 1),
       (SELECT count() FROM t_nested WHERE formatRowNoNewline('JSONEachRow', mapKeys(m)) = '{"mapKeys(m)":["k3","common"]}' SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nested WHERE formatRowNoNewline('JSONEachRow', mapKeys(m)) = '{"mapKeys(m)":["k3","common"]}' SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';
SELECT (SELECT count() FROM t_nested WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_nested WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);

-- A text index with a preprocessor over the keys.
CREATE TABLE t_text (id UInt64, m Map(LowCardinality(String), String),
    INDEX it mapKeys(m) TYPE text(tokenizer = 'array', preprocessor = lower(mapKeys(m))) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;
INSERT INTO t_text SELECT * FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_text WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_text WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Name: i%' OR explain ILIKE '%Granules: %/%';

-- A table column named like an index expression.
CREATE TABLE t_named (id UInt64, m Map(LowCardinality(String), LowCardinality(String)), `mapKeys(m)` Array(String),
    INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1, INDEX iv mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;
INSERT INTO t_named (id, m) SELECT id, m FROM t_lc SETTINGS max_block_size = 100000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT (SELECT count() FROM t_named WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_named WHERE has(mapKeys(m), 'k3') SETTINGS use_skip_indexes = 0);
SELECT (SELECT count() FROM t_named WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 1), (SELECT count() FROM t_named WHERE has(mapValues(m), 'v3') SETTINGS use_skip_indexes = 0);

DROP TABLE t_lc;
DROP TABLE t_plain;
DROP TABLE t_mut;
DROP TABLE t_order;
DROP TABLE t_mixed;
DROP TABLE t_nested;
DROP TABLE t_text;
DROP TABLE t_named;
