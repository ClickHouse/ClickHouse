-- Tags: no-parallel-replicas
-- Tests a text index with the `asciiCJK_v2` tokenizer: index creation, search functions with and without the index,
-- and granule skipping.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

SELECT '-- Index creation';

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = asciiCJK_v2)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = asciiCJK_v2())) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = 'asciiCJK_v2')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (str String, INDEX idx str TYPE text(tokenizer = asciiCJK_v2(1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT '-- Search functions return the same rows with and without the index';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = asciiCJK_v2)
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
SELECT 'hasPhrase', groupArray(id) FROM tab WHERE hasPhrase(message, 'bar baz', 'asciiCJK_v2');
SELECT 'hasPhrase', groupArray(id) FROM tab WHERE hasPhrase(message, 'bar baz', 'asciiCJK_v2') SETTINGS use_skip_indexes = 0;
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

SELECT '-- The index skips granules';

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAnyTokens(message, 'naïve'))
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name: idx%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE message LIKE '%需要处理%')
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name: idx%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAnyTokens(message, 'nonexistent'))
WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name: idx%';

DROP TABLE tab;
