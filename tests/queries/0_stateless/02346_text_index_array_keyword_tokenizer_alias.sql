-- Tests that tokenizer `keyword` is an alias of tokenizer `array`.

SELECT '-- Function tokens';

SELECT tokens('Hello World', 'array');
SELECT tokens('Hello World', 'keyword');

SELECT '-- Functions hasAllTokens/hasAnyTokens';

SELECT hasAllTokens('Hello World', ['Hello World'], 'array');
SELECT hasAllTokens('Hello World', ['Hello World'], 'keyword');
SELECT hasAnyTokens('Hello World', ['Hello'], 'array');
SELECT hasAnyTokens('Hello World', ['Hello'], 'keyword');

SELECT '-- Text index';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id UInt64,
    str String,
    INDEX idx(str) TYPE text(tokenizer = keyword))
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'Hello Elasticsearch'), (2, 'Hello Lucene');

SELECT name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

SELECT id FROM tab WHERE hasAllTokens(str, 'Hello Elasticsearch') ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(str, 'Hello Lucene') ORDER BY id;

DETACH TABLE tab;
ATTACH TABLE tab;

SELECT id FROM tab WHERE hasAllTokens(str, 'Hello Elasticsearch') ORDER BY id;

DROP TABLE tab;
