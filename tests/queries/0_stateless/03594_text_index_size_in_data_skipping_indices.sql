SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'ngram', ngram_size = 3) GRANULARITY 1,
    INDEX set_idx str TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab (str) VALUES ('I am inverted');
SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase() AND type = 'text' FORMAT Vertical;

SELECT
    partition,
    name,
    bytes_on_disk,
    secondary_indices_compressed_bytes,
    secondary_indices_uncompressed_bytes,
    secondary_indices_marks_bytes
FROM system.parts
FORMAT Vertical;

DROP TABLE tab;

SET allow_experimental_full_text_index = 0;
