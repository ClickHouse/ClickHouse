DROP TABLE IF EXISTS t_text_index_sizes;
SET enable_full_text_index = 1;

CREATE TABLE t_text_index_sizes
(
    s String,
    PROJECTION idx_s INDEX s TYPE text(tokenizer=ngrams(3))
)
ENGINE MergeTree
ORDER BY tuple();

INSERT INTO t_text_index_sizes (s) SELECT number FROM numbers(10000);

SELECT
    sum(data_compressed_bytes) <= sum(data_uncompressed_bytes)
        ? 'OK'
        : format('FAILED (compressed: {}, uncompressed: {})', sum(data_compressed_bytes), sum(data_uncompressed_bytes))
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_text_index_sizes' AND active;

DROP TABLE t_text_index_sizes;
