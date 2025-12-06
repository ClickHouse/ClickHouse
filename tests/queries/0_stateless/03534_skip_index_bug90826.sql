-- { echo ON }

SET use_skip_indexes_on_data_read = 1;
SET max_rows_to_read = 0;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `i` Int64,
    `text` String,
    INDEX bf_text text TYPE bloom_filter(0.001) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity = 4,index_granularity_bytes = 0;

INSERT INTO tab SELECT number, toString(number) FROM numbers(6); -- The Last granule contained rows smaller than index_granularity.

SELECT i, text FROM tab WHERE text = '5';

DROP TABLE tab;

CREATE TABLE tab
(
    `i` Int64,
    `text` String,
    INDEX inv_text text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity = 4, index_granularity_bytes = 0;

INSERT INTO tab SELECT number, toString(number) FROM numbers(6); -- The Last granule contained rows smaller than index_granularity.

SELECT i, text FROM tab WHERE hasToken(text, '5');

DROP TABLE tab;
