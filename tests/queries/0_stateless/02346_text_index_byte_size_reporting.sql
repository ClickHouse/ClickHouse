DROP TABLE IF EXISTS tab;

-- Tests that text indexes report correct compressed and uncompressed byte sizes.
-- Related issue: https://github.com/ClickHouse/ClickHouse/issues/87846

CREATE TABLE tab
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer=ngrams(3))
)
ENGINE MergeTree
ORDER BY tuple();

INSERT INTO tab (s) SELECT number FROM numbers(10000);

SELECT
    sum(secondary_indices_compressed_bytes) <= sum(secondary_indices_uncompressed_bytes)
        ? 'OK'
        : format('FAILED (compressed: {}, uncompressed: {})', sum(secondary_indices_compressed_bytes), sum(secondary_indices_uncompressed_bytes))
FROM system.parts
WHERE database = currentDatabase() AND table = 'tab' AND active;

DROP TABLE tab;
