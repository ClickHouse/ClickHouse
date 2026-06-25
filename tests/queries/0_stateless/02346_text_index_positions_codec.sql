-- Tags: no-parallel-replicas

-- Tests the text-index `positions_codec` parameter: 'none' and 'pfor'.

SET enable_analyzer = 1;

SELECT 'Validation errors';

CREATE TABLE tab_bad (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, positions = 1, positions_codec = 'lz4')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab_bad (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, positions_codec = 'pfor')
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT 'Results are same with both positions_codec parameters';

CREATE TABLE tab_none (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, positions = 1, positions_codec = 'none')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

CREATE TABLE tab_pfor (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, positions = 1, positions_codec = 'pfor')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

-- A few curated rows covering varied phrase cases (order, single token, no match) ...
INSERT INTO tab_none(id, message) VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc baz foo'),
    (4, 'abc baz bar'),
    (5, 'zzz foo bar'),
    (6, 'foo bar baz qux');

-- ... plus many rows so the token positions span multiple pfor blocks (ids offset past the curated ones).
INSERT INTO tab_none SELECT number + 10, 'hello clickhouse world' FROM numbers(2048);
INSERT INTO tab_none SELECT number + 2058, 'hello world clickhouse' FROM numbers(2048);

INSERT INTO tab_pfor(id, message) SELECT id, message FROM tab_none;

SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'abc def'))     = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'abc def'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'def foo'))     = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'def foo'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'abc baz'))     = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'abc baz'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'baz bar'))     = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'baz bar'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'foo bar baz')) = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'foo bar baz'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'abc def foo')) = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'abc def foo'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'def abc'))     = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'def abc'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'qux'))         = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'qux'));
SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'nothing here'))= (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'nothing here'));

SELECT (SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse')) = (SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse'));
SELECT (SELECT count() FROM tab_none WHERE hasPhrase(message, 'clickhouse world')) = (SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'clickhouse world'));
SELECT (SELECT count() FROM tab_none WHERE hasPhrase(message, 'world clickhouse')) = (SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'world clickhouse'));

DROP TABLE tab_none;
DROP TABLE tab_pfor;

SELECT 'No token positions limit';

CREATE TABLE tab_none (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, positions = 1, positions_codec = 'none')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

CREATE TABLE tab_pfor (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, positions = 1, positions_codec = 'pfor')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_positions = 1;

-- ~2M filler tokens then a planted phrase well past position 1,048,576.
INSERT INTO tab_none(id, message) SELECT 1, concat(arrayStringConcat(arrayMap(x -> 'w', range(2000000)), ' '), ' needle haystack');
INSERT INTO tab_pfor(id, message) SELECT id, message FROM tab_none;

SELECT (SELECT groupArray(id) FROM tab_none WHERE hasPhrase(message, 'needle haystack')) = (SELECT groupArray(id) FROM tab_pfor WHERE hasPhrase(message, 'needle haystack'));

SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'haystack needle');

DROP TABLE tab_none;
DROP TABLE tab_pfor;
