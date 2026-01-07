-- Tags: no-parallel-replicas

SET enable_full_text_index = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    text String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, merge_max_block_size = 8192;

INSERT INTO tab SELECT number, 'v' || toString(number) FROM numbers(100000);

ALTER TABLE tab ADD INDEX idx_text (text) TYPE text(tokenizer = ngrams(3));

INSERT INTO tab SELECT number, 'v' || toString(number + 1000000) FROM numbers(100000);

-- ------------------------------------------------------------
SELECT 'Before OPTIMIZE FINAL';

SELECT secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY name;

SELECT count() FROM tab WHERE text LIKE '%v322%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%filter column%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%Granules%';

CHECK TABLE tab SETTINGS check_query_single_value_result = 1;

-- ------------------------------------------------------------
OPTIMIZE TABLE tab FINAL;
SELECT 'After OPTIMIZE FINAL';

SELECT secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY name;

SELECT count() FROM tab WHERE text LIKE '%v322%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%filter column%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%Granules%';

CHECK TABLE tab SETTINGS check_query_single_value_result = 1;

-- ------------------------------------------------------------
SET mutations_sync = 2;
ALTER TABLE tab CLEAR INDEX idx_text;
SELECT 'After CLEAR INDEX idx_text';

SELECT secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY name;

SELECT count() FROM tab WHERE text LIKE '%v322%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%filter column%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%Granules%';

CHECK TABLE tab SETTINGS check_query_single_value_result = 1;

-- ------------------------------------------------------------
ALTER TABLE tab MATERIALIZE INDEX idx_text;
SELECT 'After MATERIALIZE INDEX idx_text';

SELECT secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY name;

SELECT count() FROM tab WHERE text LIKE '%v322%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%filter column%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM tab WHERE text LIKE '%v322%'
)
WHERE explain ILIKE '%Granules%';

CHECK TABLE tab SETTINGS check_query_single_value_result = 1;

DROP TABLE tab;
