-- Tags: no-parallel-replicas
-- Tests a text index with the `stringzilla` tokenizer: index creation, search functions with and without the index,
-- and granule skipping.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

SELECT '-- Index creation';

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = stringzilla)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = stringzilla())) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = 'stringzilla')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = stringzilla(1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT '-- Search functions return the same rows with and without the index';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = stringzilla)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (1, 'hello错误502需要处理kitty'),
    (2, 'café naïve'),
    (3, '한국어 텍스트 검색'),
    (4, 'ClickHouse是一个用于OLAP的列式数据库'),
    (5, 'version 3.14 of foo.bar is out'),
    (6, 'foo bar baz'),
    (7, 'the_user_id is 42'),
    (8, 'line1\r\nline2'),
    (9, '50% off: a_b c\\d'),
    (10, 'emoji 😀 party');

-- Each query runs with the index and as a brute-force scan; the results must match.
SELECT 'hasToken', groupArray(id) FROM tab WHERE hasToken(message, 'café');
SELECT 'hasToken', groupArray(id) FROM tab WHERE hasToken(message, 'café') SETTINGS use_skip_indexes = 0;
SELECT 'hasAnyTokens', groupArray(id) FROM tab WHERE hasAnyTokens(message, ['한국어', 'kitty']);
SELECT 'hasAnyTokens', groupArray(id) FROM tab WHERE hasAnyTokens(message, ['한국어', 'kitty']) SETTINGS use_skip_indexes = 0;
SELECT 'hasAllTokens', groupArray(id) FROM tab WHERE hasAllTokens(message, '数据库 OLAP');
SELECT 'hasAllTokens', groupArray(id) FROM tab WHERE hasAllTokens(message, '数据库 OLAP') SETTINGS use_skip_indexes = 0;
SELECT 'hasAllTokens', groupArray(id) FROM tab WHERE hasAllTokens(message, ['3.14', 'foo.bar']);
SELECT 'hasAllTokens', groupArray(id) FROM tab WHERE hasAllTokens(message, ['3.14', 'foo.bar']) SETTINGS use_skip_indexes = 0;
SELECT 'hasPhrase', groupArray(id) FROM tab WHERE hasPhrase(message, 'bar baz', 'stringzilla');
SELECT 'hasPhrase', groupArray(id) FROM tab WHERE hasPhrase(message, 'bar baz', 'stringzilla') SETTINGS use_skip_indexes = 0;
SELECT 'equals', groupArray(id) FROM tab WHERE message = 'foo bar baz';
SELECT 'equals', groupArray(id) FROM tab WHERE message = 'foo bar baz' SETTINGS use_skip_indexes = 0;

SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%错误502需要%';
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%错误502需要%' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%user\\_id is%';
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%user\\_id is%' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '50\\% off: a\\_b c\\\\d';
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '50\\% off: a\\_b c\\\\d' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%한국어 텍스트%';
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%한국어 텍스트%' SETTINGS use_skip_indexes = 0;
SELECT 'notLike', groupArray(id) FROM tab WHERE message NOT LIKE '%foo bar%';
SELECT 'notLike', groupArray(id) FROM tab WHERE message NOT LIKE '%foo bar%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike', groupArray(id) FROM tab WHERE message ILIKE '%CAFÉ NAÏVE%';
SELECT 'ilike', groupArray(id) FROM tab WHERE message ILIKE '%CAFÉ NAÏVE%' SETTINGS use_skip_indexes = 0;
SELECT 'startsWith', groupArray(id) FROM tab WHERE startsWith(message, 'version 3.1');
SELECT 'startsWith', groupArray(id) FROM tab WHERE startsWith(message, 'version 3.1') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith', groupArray(id) FROM tab WHERE endsWith(message, ' is out');
SELECT 'endsWith', groupArray(id) FROM tab WHERE endsWith(message, ' is out') SETTINGS use_skip_indexes = 0;
SELECT 'match', groupArray(id) FROM tab WHERE match(message, 'line1\\r\\nline2');
SELECT 'match', groupArray(id) FROM tab WHERE match(message, 'line1\\r\\nline2') SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAny', groupArray(id) FROM tab WHERE multiSearchAny(message, ['的列式', 'party']);
SELECT 'multiSearchAny', groupArray(id) FROM tab WHERE multiSearchAny(message, ['的列式', 'party']) SETTINGS use_skip_indexes = 0;

SELECT '-- Tokens that the text around a pattern may extend are not looked up';

-- UAX #29 joins words across punctuation (`foo.bar`, `1,000`, `3;4`, `don't`), so `bar`, `000` or `t` next to a wildcard
-- or to the open side of a substring can be a part of a longer token of the row.
CREATE TABLE tab_edges
(
    id UInt32,
    message String,
    keys Map(String, UInt8),
    INDEX idx(message) TYPE text(tokenizer = stringzilla),
    INDEX idx_keys(mapKeys(keys)) TYPE text(tokenizer = stringzilla)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_edges VALUES
    (1, 'version 3.14 of foo.bar is out', {'foo.bar is out': 1}),
    (2, 'loaded 1,000 rows', {'loaded 1,000 rows': 1}),
    (3, 'ratio 3;4 kept', {'ratio 3;4 kept': 1}),
    (4, 'I don''t know', {'I don''t know': 1}),
    (5, 'foo bar baz', {'foo bar baz': 1});

SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%.bar is%';
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%.bar is%' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%of foo.%';
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%of foo.%' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%,000 rows%';
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%,000 rows%' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%loaded 1,%';
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%loaded 1,%' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%;4 kept';
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%;4 kept' SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE $$%'t know%$$;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE $$%'t know%$$ SETTINGS use_skip_indexes = 0;
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%foo bar%';
SELECT 'like', groupArray(id) FROM tab_edges WHERE message LIKE '%foo bar%' SETTINGS use_skip_indexes = 0;
SELECT 'startsWith', groupArray(id) FROM tab_edges WHERE startsWith(message, 'ratio 3;');
SELECT 'startsWith', groupArray(id) FROM tab_edges WHERE startsWith(message, 'ratio 3;') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith', groupArray(id) FROM tab_edges WHERE endsWith(message, '.bar is out');
SELECT 'endsWith', groupArray(id) FROM tab_edges WHERE endsWith(message, '.bar is out') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith', groupArray(id) FROM tab_edges WHERE endsWith(message, ',000 rows');
SELECT 'endsWith', groupArray(id) FROM tab_edges WHERE endsWith(message, ',000 rows') SETTINGS use_skip_indexes = 0;
SELECT 'match', groupArray(id) FROM tab_edges WHERE match(message, ',000 rows');
SELECT 'match', groupArray(id) FROM tab_edges WHERE match(message, ',000 rows') SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAny', groupArray(id) FROM tab_edges WHERE multiSearchAny(message, [',000 rows', '.bar is']);
SELECT 'multiSearchAny', groupArray(id) FROM tab_edges WHERE multiSearchAny(message, [',000 rows', '.bar is']) SETTINGS use_skip_indexes = 0;
SELECT 'mapContainsKeyLike', groupArray(id) FROM tab_edges WHERE mapContainsKeyLike(keys, '%,000 rows%');
SELECT 'mapContainsKeyLike', groupArray(id) FROM tab_edges WHERE mapContainsKeyLike(keys, '%,000 rows%') SETTINGS use_skip_indexes = 0;

-- Tokens separated from the open side by whitespace are still looked up.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_edges WHERE message LIKE '%,000 rows loaded%')
WHERE explain LIKE '%Condition:%' AND explain LIKE '%tokens%';

DROP TABLE tab_edges;

SELECT '-- The index skips granules';

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAnyTokens(message, 'naïve'))
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name: idx%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE message LIKE '%需要处理%')
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name: idx%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAnyTokens(message, 'nonexistent'))
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name: idx%';

DROP TABLE tab;

SELECT '-- Segments are fetched in batches: restarting inside a string gives the same tokens';

-- Tokens never span whitespace, so tokenizing a concatenation must give the concatenated tokens. The random mixed-script
-- strings are long enough to cross many batch boundaries.
SELECT countIf(tokens(a || ' ' || b, 'stringzilla') != arrayConcat(tokens(a, 'stringzilla'), tokens(b, 'stringzilla')))
FROM
(
    SELECT
        arrayStringConcat(arrayMap(x -> ['foo', 'bar.baz', '1,000', '中', '文', 'テキスト', 'ひ', '한국어', 'café', ', ', '.', '😀', '_', '\r\n', ' '][(cityHash64(number, x) % 15) + 1], range(300)), '') AS a,
        arrayStringConcat(arrayMap(x -> ['foo', 'bar.baz', '1,000', '中', '文', 'テキスト', 'ひ', '한국어', 'café', ', ', '.', '😀', '_', '\r\n', ' '][(cityHash64(number, x, 1) % 15) + 1], range(200)), '') AS b
    FROM numbers(2000)
);

SELECT '-- Tokenization is the same in one and in many threads';

SELECT
    (SELECT groupBitXor(cityHash64(tokens(s, 'stringzilla'))) FROM (SELECT concat('row ', toString(number), ' 中文 café, 한국어.', repeat('x y ', number % 50)) AS s FROM numbers_mt(100000)) SETTINGS max_threads = 1)
  = (SELECT groupBitXor(cityHash64(tokens(s, 'stringzilla'))) FROM (SELECT concat('row ', toString(number), ' 中文 café, 한국어.', repeat('x y ', number % 50)) AS s FROM numbers_mt(100000)) SETTINGS max_threads = 8, max_block_size = 1000);
