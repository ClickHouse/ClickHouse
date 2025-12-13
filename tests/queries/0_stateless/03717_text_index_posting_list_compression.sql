-- { echo ON }

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE default.tab
(
    `id` UInt64,
    `text` String,
    INDEX inv_idx text TYPE text(tokenizer = 'splitByNonAlpha', enable_postings_compression = true) GRANULARITY 128
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, multiIf(number % 10 == 0, 'aa', number % 10 == 1, 'bb', number % 10 == 2, 'cc', 'dd') FROM numbers (102400);

SELECT count() FROM tab WHERE hasToken(text, 'bb');

SELECT count() FROM tab WHERE hasToken(text, 'cc');

SELECT count() FROM tab WHERE hasAllTokens(text, ['aa']);
