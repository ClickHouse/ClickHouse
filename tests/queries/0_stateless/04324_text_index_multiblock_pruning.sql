SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_multiblock_pruning;

CREATE TABLE tab_multiblock_pruning
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = 'array', posting_list_block_size = 128, posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_multiblock_pruning
SELECT number, concat('v', toString((number % 4) + 1))
FROM numbers(2000);

OPTIMIZE TABLE tab_multiblock_pruning FINAL;

SELECT token, cardinality, num_posting_blocks
FROM mergeTreeTextIndex(currentDatabase(), tab_multiblock_pruning, idx_s)
WHERE token IN ('v1', 'v2', 'v3', 'v4')
ORDER BY token
SETTINGS max_rows_to_read = 8;

SELECT count()
FROM tab_multiblock_pruning
WHERE hasAllTokens(s, ['v1', 'v2', 'v3', 'v4']);

SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM tab_multiblock_pruning
    WHERE hasAllTokens(s, ['v1', 'v2', 'v3', 'v4'])
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

DROP TABLE tab_multiblock_pruning;
