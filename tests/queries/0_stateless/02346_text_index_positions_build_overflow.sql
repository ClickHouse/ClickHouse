-- Tags: no-parallel-replicas

-- Building a text index with support_phrase_search = 1 over many distinct short tokens must not
-- read out of bounds. Granule build looked the token up again in the positions hash map, but the token view
-- points into the postings hash table's own cell storage (not 8-byte padded), and StringHashTable
-- lookup reads 8 bytes around the key, so a short token near a buffer boundary triggered a
-- heap-buffer-overflow under ASan. A large token set is needed to grow the hash table so a short
-- token lands at such a boundary.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS allow_experimental_text_index_phrase_search = 1, index_granularity = 100000;

INSERT INTO tab
SELECT
    number AS id,
    arrayStringConcat(arrayMap(x -> concat('t', toString(x)), range(1, 3000)), ' ') AS message
FROM numbers(20);

SELECT count() FROM tab;

-- Positions are still attached correctly: phrase search over consecutive short tokens works.
SELECT count() FROM tab WHERE hasPhrase(message, 't1 t2 t3');
SELECT count() FROM tab WHERE hasPhrase(message, 't1 t3');

DROP TABLE tab;
