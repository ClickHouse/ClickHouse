DROP TABLE IF EXISTS ttl_clear_text_small_big;
DROP TABLE IF EXISTS ttl_clear_text_transition;

SET enable_full_text_index = 1;

CREATE TABLE ttl_clear_text_small_big
(
    d Date,
    id UInt64,
    s String,
    INDEX idx_text s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY id
TTL d + INTERVAL 1 DAY CLEAR INDEX idx_text
SETTINGS
    index_granularity = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO ttl_clear_text_small_big
SELECT
    toDate('2000-01-01'),
    number,
    'old-big-token-' || toString(number)
FROM numbers(10000);

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_text_small_big'
  AND active;

OPTIMIZE TABLE ttl_clear_text_small_big FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_text_small_big'
  AND active;

INSERT INTO ttl_clear_text_small_big VALUES
    ('2000-01-01', 100000, 'tiny-late-token'),
    ('2000-01-01', 100001, 'tiny-late-token-2');

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_text_small_big'
  AND active;

OPTIMIZE TABLE ttl_clear_text_small_big FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 0;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_text_small_big'
  AND active;

SELECT count()
FROM ttl_clear_text_small_big
WHERE s LIKE '%tiny-late-token%';

DROP TABLE ttl_clear_text_small_big;

CREATE TABLE ttl_clear_text_transition
(
    d Date,
    id UInt64,
    s String,
    INDEX idx_text s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY id
SETTINGS
    index_granularity = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO ttl_clear_text_transition
SELECT
    toDate('2000-01-01'),
    number,
    'transition-token-' || toString(number)
FROM numbers(10000);

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_text_transition'
  AND active;

ALTER TABLE ttl_clear_text_transition
    MODIFY TTL d + INTERVAL 1 DAY CLEAR INDEX idx_text
    SETTINGS mutations_sync = 2;

OPTIMIZE TABLE ttl_clear_text_transition FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 0;

SELECT sum(secondary_indices_compressed_bytes) > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_text_transition'
  AND active;

SELECT count()
FROM ttl_clear_text_transition
WHERE s = 'transition-token-42';

DROP TABLE ttl_clear_text_transition;
