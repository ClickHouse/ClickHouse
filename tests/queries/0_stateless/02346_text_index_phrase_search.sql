-- Tags: no-parallel-replicas

-- Candidate-driven per-document positions: phrase search
-- through the index must return exactly the same results as brute-force hasPhrase on an index-less
-- table. posting_list_block_size is lowered so token postings span many blocks (the
-- posting-rank space positions are addressed in), and curated rows cover repeated tokens
-- (frequency exceptions) and 3-term phrases.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;

SELECT 'Validation';

-- The positions layout is not user-configurable: positions_codec is an unknown argument.
CREATE TABLE tab_bad (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, positions_codec = 'pfor')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

SELECT 'Results are same as brute force';

CREATE TABLE tab_ref (
    id UInt32,
    message String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_idx (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, posting_list_block_size = 256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

-- Curated rows: phrase order, repeated tokens (multi-occurrence -> frequency exceptions), no match ...
INSERT INTO tab_ref(id, message) VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc baz foo'),
    (4, 'foo foo foo abc def'),
    (5, 'zzz foo bar'),
    (6, 'foo bar baz qux');

-- ... plus many rows so postings span multiple 256-entry blocks and positions span many
-- 128-document position blocks (ids offset past the curated ones).
INSERT INTO tab_ref SELECT number + 10, 'hello clickhouse world' FROM numbers(2048);
INSERT INTO tab_ref SELECT number + 2058, 'hello world clickhouse' FROM numbers(2048);

INSERT INTO tab_idx SELECT id, message FROM tab_ref;

SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'abc def'))      = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'abc def'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'def foo'))      = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'def foo'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'abc baz'))      = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'abc baz'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo bar baz'))  = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo bar baz'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'abc def foo'))  = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'abc def foo'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo foo'))      = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo foo'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo foo foo'))  = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo foo foo'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'def abc'))      = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'def abc'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'qux'))          = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'qux'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'nothing here')) = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'nothing here'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo bar baz qux'))     = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo bar baz qux'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo bar baz zzz'))     = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo bar baz zzz'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo foo foo abc def')) = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo foo foo abc def'));
SELECT (SELECT groupArray(id) FROM tab_ref WHERE hasPhrase(message, 'foo foo abc def'))     = (SELECT groupArray(id) FROM tab_idx WHERE hasPhrase(message, 'foo foo abc def'));

SELECT (SELECT count() FROM tab_ref WHERE hasPhrase(message, 'hello clickhouse')) = (SELECT count() FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse'));
SELECT (SELECT count() FROM tab_ref WHERE hasPhrase(message, 'clickhouse world')) = (SELECT count() FROM tab_idx WHERE hasPhrase(message, 'clickhouse world'));
SELECT (SELECT count() FROM tab_ref WHERE hasPhrase(message, 'world clickhouse')) = (SELECT count() FROM tab_idx WHERE hasPhrase(message, 'world clickhouse'));

SELECT 'Direct index read';

SELECT count() FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse');
SELECT count() FROM tab_idx WHERE hasPhrase(message, 'foo foo');
SELECT count() FROM tab_idx WHERE hasPhrase(message, 'world hello');

DROP TABLE tab_ref;
DROP TABLE tab_idx;

SELECT 'Merge path re-encodes positions';

CREATE TABLE tab_m_ref (
    id UInt32,
    message String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_m_idx (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, posting_list_block_size = 256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

-- Stop background merges so the separate INSERTs stay as separate parts until the explicit OPTIMIZE.
SYSTEM STOP MERGES tab_m_idx;

INSERT INTO tab_m_idx(id, message) VALUES (1, 'abc def foo'), (2, 'abc def bar'), (3, 'zzz foo bar'), (4, 'foo foo foo abc def'), (5, 'needle clickhouse');
INSERT INTO tab_m_idx SELECT number + 10, 'hello clickhouse world' FROM numbers(2048);
INSERT INTO tab_m_idx SELECT number + 3000, 'hello world clickhouse' FROM numbers(2048);
INSERT INTO tab_m_ref SELECT id, message FROM tab_m_idx;

-- At least two active parts before the merge.
SELECT count() >= 2 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_m_idx' AND active;

-- Phrase results across the multiple parts match the brute-force ground truth (pre-merge).
SELECT (SELECT arraySort(groupArray(id)) FROM tab_m_ref WHERE hasPhrase(message, 'abc def'))  = (SELECT arraySort(groupArray(id)) FROM tab_m_idx WHERE hasPhrase(message, 'abc def'));
SELECT (SELECT count() FROM tab_m_ref WHERE hasPhrase(message, 'hello clickhouse'))           = (SELECT count() FROM tab_m_idx WHERE hasPhrase(message, 'hello clickhouse'));

SYSTEM START MERGES tab_m_idx;
OPTIMIZE TABLE tab_m_idx FINAL;

-- Merged into a single part; the merge paired the per-rank lists with the postings,
-- remapped the row ids and re-encoded the positions stream.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_m_idx' AND active;

SELECT (SELECT arraySort(groupArray(id)) FROM tab_m_ref WHERE hasPhrase(message, 'abc def'))  = (SELECT arraySort(groupArray(id)) FROM tab_m_idx WHERE hasPhrase(message, 'abc def'));
SELECT (SELECT count() FROM tab_m_ref WHERE hasPhrase(message, 'hello clickhouse'))           = (SELECT count() FROM tab_m_idx WHERE hasPhrase(message, 'hello clickhouse'));
SELECT (SELECT count() FROM tab_m_ref WHERE hasPhrase(message, 'world clickhouse'))           = (SELECT count() FROM tab_m_idx WHERE hasPhrase(message, 'world clickhouse'));
SELECT (SELECT groupArray(id) FROM tab_m_ref WHERE hasPhrase(message, 'foo foo'))             = (SELECT groupArray(id) FROM tab_m_idx WHERE hasPhrase(message, 'foo foo'));

SELECT 'Blocks are read and skipped through the index';

SELECT count() FROM tab_m_idx WHERE hasPhrase(message, 'needle clickhouse') SETTINGS log_comment = '02346_phrase_search_needle';

SYSTEM FLUSH LOGS query_log;

SELECT 
    ProfileEvents['TextIndexPositionsBlocksRead'] > 0,
    ProfileEvents['TextIndexPositionsBlocksRead'] < ProfileEvents['TextIndexPositionsBlocksTotal']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '02346_phrase_search_needle'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE tab_m_ref;
DROP TABLE tab_m_idx;
