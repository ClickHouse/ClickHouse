SET allow_experimental_full_text_index = 1;

SELECT 'Use raw tokens to store the dictionary block';

DROP TABLE IF EXISTS table_raw;

CREATE TABLE table_raw
(
    id UInt32,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'default', dictionary_block_frontcoding_compression = 0),
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO table_raw VALUES (0, 'foo'), (1, 'bar'), (2, 'baz'), (3, 'foo bar'), (4, 'foo baz'), (5, 'bar baz'), (6, 'abc'), (7, 'def');

SELECT count() FROM table_raw WHERE hasToken(text, 'foo');
SELECT count() FROM table_raw WHERE hasToken(text, 'bar');
SELECT count() FROM table_raw WHERE hasToken(text, 'abc');

DROP TABLE table_raw;

SELECT 'Use frontcoding encoded tokens to store the dictionary block';

DROP TABLE IF EXISTS table_frontcoding;
CREATE TABLE table_frontcoding
(
    id UInt32,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'default', dictionary_block_frontcoding_compression = 1),
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO table_frontcoding VALUES (0, 'foo'), (1, 'bar'), (2, 'baz'), (3, 'foo bar'), (4, 'foo baz'), (5, 'bar baz'), (6, 'abc'), (7, 'def');

SELECT count() FROM table_frontcoding WHERE hasToken(text, 'foo');
SELECT count() FROM table_frontcoding WHERE hasToken(text, 'bar');
SELECT count() FROM table_frontcoding WHERE hasToken(text, 'abc');

DROP TABLE table_frontcoding;
