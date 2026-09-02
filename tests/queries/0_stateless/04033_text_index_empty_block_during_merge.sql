DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    key UInt32,
    text String,
    dt DateTime,
)
ENGINE = MergeTree
ORDER BY id
TTL dt + INTERVAL 1 MONTH DELETE WHERE key < 5
SETTINGS merge_max_block_size = 1024;

SYSTEM STOP MERGES tab;

INSERT INTO tab (id, key, text, dt)
SELECT number, number / 5000, 'hello world', toDateTime('2000-01-01 00:00:00')
FROM numbers(50000);

ALTER TABLE tab ADD INDEX idx_text(text) TYPE text(tokenizer = 'splitByNonAlpha');

SYSTEM START MERGES tab;

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE hasAllTokens(text, 'hello');

DROP TABLE tab;
