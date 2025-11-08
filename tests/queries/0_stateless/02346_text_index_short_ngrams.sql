SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    arr Array(String),
    map Map(String, String),
    INDEX idx(`message`) TYPE text(tokenizer = ngrams(4)),
    INDEX array_idx(arr) TYPE text(tokenizer = ngrams(4)),
    INDEX map_keys_idx mapKeys(map) TYPE text(tokenizer = ngrams(4)),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
VALUES
(1, 'abc', ['abc'], {'abc':'abc'}),
(2, 'def', ['def', 'abc'], {'abc':'foo', 'def':'foo'});

-- { echoOn }
SELECT * FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT * FROM tab WHERE hasAllTokens(message, ['abc']);

SELECT * FROM tab WHERE hasAnyTokens(message, ['']);
SELECT * FROM tab WHERE hasAllTokens(message, ['']);

SELECT * FROM tab WHERE hasAnyTokens(message, []);
SELECT * FROM tab WHERE hasAllTokens(message, []);

SELECT * FROM tab WHERE hasAnyTokens(message, 'abc');
SELECT * FROM tab WHERE hasAllTokens(message, 'abc');

SELECT * FROM tab WHERE hasAnyTokens(message, '');
SELECT * FROM tab WHERE hasAllTokens(message, '');

SELECT tokens('abc', 'ngrams', 4) AS tokens;

SELECT * from tab WHERE hasToken(message, 'abc');

-- Arrays
SELECT * FROM tab WHERE hasAnyTokens(arr, ['abc']);
SELECT * FROM tab WHERE hasAllTokens(arr, ['abc']);

SELECT * FROM tab WHERE hasAnyTokens(arr, ['']);
SELECT * FROM tab WHERE hasAllTokens(arr, ['']);

SELECT * FROM tab WHERE hasAnyTokens(arr, []);
SELECT * FROM tab WHERE hasAllTokens(arr, []);

SELECT * FROM tab WHERE hasAnyTokens(arr, 'abc');
SELECT * FROM tab WHERE hasAllTokens(arr, 'abc');

SELECT * FROM tab WHERE hasAnyTokens(arr, '');
SELECT * FROM tab WHERE hasAllTokens(arr, '');

SELECT * from tab WHERE has(arr, 'abc');

-- Maps
SELECT * FROM tab WHERE hasAnyTokens(mapKeys(map), ['abc']);
SELECT * FROM tab WHERE hasAllTokens(mapKeys(map), ['abc']);

SELECT * FROM tab WHERE hasAnyTokens(mapKeys(map), ['']);
SELECT * FROM tab WHERE hasAllTokens(mapKeys(map), ['']);

SELECT * FROM tab WHERE hasAnyTokens(mapKeys(map), []);
SELECT * FROM tab WHERE hasAllTokens(mapKeys(map), []);

SELECT * FROM tab WHERE hasAnyTokens(mapKeys(map), 'abc');
SELECT * FROM tab WHERE hasAllTokens(mapKeys(map), 'abc');

SELECT * FROM tab WHERE hasAnyTokens(mapKeys(map), '');
SELECT * FROM tab WHERE hasAllTokens(mapKeys(map), '');

SELECT * FROM tab WHERE mapContains(map, 'abc');
SELECT * FROM tab WHERE mapContains(map, '');

SELECT * from tab WHERE has(map, 'abc');
-- { echoOff }

DROP TABLE tab;
