-- Test that the text index works correctly when the number of rows in a part is smaller than the index_granularity.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `id`   UInt64,
    `text` String,
    INDEX inv_idx text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 32;

INSERT INTO tab VALUES (0,'a'),(1,'b'),(2,'c'),(3,'d');

SELECT id FROM tab WHERE hasToken(text, 'b');

SELECT id FROM tab WHERE hasToken(text, 'c');

INSERT INTO tab SELECT number , 'aaabbbccc' FROM numbers(128);

SELECT id FROM tab WHERE hasToken(text, 'aaabbbccc');
