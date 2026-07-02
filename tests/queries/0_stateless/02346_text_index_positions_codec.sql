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

SELECT 'Merge path re-encodes positions';

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

-- Stop background merges so the separate INSERTs stay as separate parts until the explicit OPTIMIZE.
SYSTEM STOP MERGES tab_pfor;

-- One part per INSERT; positions span multiple pfor blocks so the merge re-encodes real block streams.
INSERT INTO tab_pfor(id, message) VALUES (1, 'abc def foo'), (2, 'abc def bar'), (3, 'zzz foo bar');
INSERT INTO tab_pfor SELECT number + 10, 'hello clickhouse world' FROM numbers(2048);
INSERT INTO tab_pfor SELECT number + 3000, 'hello world clickhouse' FROM numbers(2048);
INSERT INTO tab_none SELECT id, message FROM tab_pfor;

-- At least two active pfor parts before the merge.
SELECT count() >= 2 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_pfor' AND active;

-- Phrase results across the multiple pfor parts match the 'none' ground truth (pre-merge).
SELECT (SELECT arraySort(groupArray(id)) FROM tab_none WHERE hasPhrase(message, 'abc def'))   = (SELECT arraySort(groupArray(id)) FROM tab_pfor WHERE hasPhrase(message, 'abc def'));
SELECT (SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse'))            = (SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse'));

OPTIMIZE TABLE tab_pfor FINAL;

-- Merged into a single part; the merge decoded and re-encoded the pfor positions.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_pfor' AND active;

-- Same phrase results after the merge re-encoded the positions on the merged part.
SELECT (SELECT arraySort(groupArray(id)) FROM tab_none WHERE hasPhrase(message, 'abc def'))   = (SELECT arraySort(groupArray(id)) FROM tab_pfor WHERE hasPhrase(message, 'abc def'));
SELECT (SELECT count() FROM tab_none WHERE hasPhrase(message, 'hello clickhouse'))            = (SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'hello clickhouse'));
SELECT (SELECT count() FROM tab_none WHERE hasPhrase(message, 'world clickhouse'))            = (SELECT count() FROM tab_pfor WHERE hasPhrase(message, 'world clickhouse'));

DROP TABLE tab_none;
DROP TABLE tab_pfor;
