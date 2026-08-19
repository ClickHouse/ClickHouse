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

CREATE TABLE tab (id UInt64, arr Array(String), INDEX idx(arr) TYPE text(tokenizer = keyword))
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, ['Hello World', 'ClickHouse']), (2, ['Hello', 'World']);

SELECT name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

-- Each array element becomes a single token, same as with tokenizer `array`.
SELECT id FROM tab WHERE has(arr, 'Hello World') ORDER BY id;
SELECT id FROM tab WHERE has(arr, 'Hello') ORDER BY id;

-- The table metadata stores the alias verbatim, check that it can be re-parsed.
DETACH TABLE tab;
ATTACH TABLE tab;

SELECT id FROM tab WHERE has(arr, 'Hello World') ORDER BY id;

DROP TABLE tab;
