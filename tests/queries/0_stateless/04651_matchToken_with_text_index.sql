-- Text-index behavior tests for matchToken: EXPLAIN granule filtering,
-- Array index hit, and prefix range-scan correctness.
-- The no-index correctness sibling is 04651_matchToken.sql.

SET explain_query_plan_default = 'legacy';

SET enable_full_text_index = 1;

-- ============================================================
-- §1 EXPLAIN granule filtering (scalar text index)
-- ============================================================
DROP TABLE IF EXISTS test_match_token;

CREATE TABLE test_match_token
(
    id UInt32,
    message String,
    INDEX idx_msg(message) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_match_token VALUES
    (1, 'error occurred in the system'),
    (2, 'hello world from clickhouse'),
    (3, 'errno is set to invalid value'),
    (4, 'connection error handler'),
    (5, 'nothing matches here'),
    (6, ''),
    (7, 'ERRor case sensitive test'),
    (8, 'test123 numeric token'),
    (9, 'err_code is 42'),
    (10, 'warnings and errors everywhere'),
    (11, 'cafe resume naive');

SELECT '-- EXPLAIN: regexp err.*';
SELECT trimLeft(explain) AS explain FROM (EXPLAIN indexes = 1 SELECT * FROM test_match_token WHERE matchToken(message, 'err.*'))
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT '-- EXPLAIN: regexp xyz.* (no match)';
SELECT trimLeft(explain) AS explain FROM (EXPLAIN indexes = 1 SELECT * FROM test_match_token WHERE matchToken(message, 'xyz.*'))
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT '-- setting off: text index is not used';
SET use_text_index_match_token_evaluation_by_dictionary_scan = 0;
SELECT count() FROM (EXPLAIN indexes = 1 SELECT * FROM test_match_token WHERE matchToken(message, 'err.*'))
WHERE explain LIKE '%Name: idx_msg%';
SET use_text_index_match_token_evaluation_by_dictionary_scan = 1;

DROP TABLE test_match_token;

-- ============================================================
-- §2 Array(String) text index: index hit
-- ============================================================
DROP TABLE IF EXISTS test_match_token_array;

CREATE TABLE test_match_token_array
(
    id UInt32,
    tags Array(String),
    INDEX idx_tags(tags) TYPE text(tokenizer = 'asciiCJK', preprocessor = lower(tags))
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_match_token_array VALUES
    (1, ['hello world', 'error handler']),
    (2, ['alpha beta', 'gamma delta']),
    (3, ['errno state', 'misc value']),
    (4, ['ERROR handler']);

SELECT '-- text index on Array(String): uses text index';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM test_match_token_array WHERE matchToken(tags, 'err.*')
)
WHERE explain LIKE '%Skip%' OR explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Condition:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%';

SELECT id, tags FROM test_match_token_array WHERE matchToken(tags, 'err.*') ORDER BY id;

SELECT '-- preprocessor fallback: direct read off';
SET query_plan_direct_read_from_text_index = 0;
SELECT id, tags FROM test_match_token_array WHERE matchToken(tags, '^error$') ORDER BY id;
SET query_plan_direct_read_from_text_index = 1;

SELECT '-- explicit tokenizer with preprocessor: preserve standalone semantics and bypass index';
SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT * FROM test_match_token_array WHERE matchToken(tags, '^ERROR$', 'asciiCJK')
)
WHERE explain LIKE '%Name: idx_tags%';
SELECT id FROM test_match_token_array WHERE matchToken(tags, '^ERROR$', 'asciiCJK') ORDER BY id;

DROP TABLE test_match_token_array;

-- ============================================================
-- §3 Postprocessor fallback and explicit tokenizer guards
-- ============================================================
DROP TABLE IF EXISTS test_match_token_pipeline;

CREATE TABLE test_match_token_pipeline
(
    id UInt32,
    message String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

-- Keep one part without the materialized index and one part with it. Both must use the index pipeline semantics.
INSERT INTO test_match_token_pipeline VALUES (1, 'ERROR handler');
ALTER TABLE test_match_token_pipeline ADD INDEX idx_pipeline(message)
    TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(message));
INSERT INTO test_match_token_pipeline VALUES (2, 'ERROR state'), (3, 'other value');

SELECT '-- postprocessor: index path';
SELECT id FROM test_match_token_pipeline WHERE matchToken(message, '^error$') ORDER BY id;

SELECT '-- postprocessor fallback: direct read off';
SET query_plan_direct_read_from_text_index = 0;
SELECT id FROM test_match_token_pipeline WHERE matchToken(message, '^error$') ORDER BY id;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE test_match_token_pipeline;

DROP TABLE IF EXISTS test_match_token_explicit;

CREATE TABLE test_match_token_explicit
(
    id UInt32,
    message String,
    INDEX idx_explicit(message) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_match_token_explicit VALUES
    (1, 'error'),
    (2, 'error code');

SELECT '-- explicit matching tokenizer: uses text index';
SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT * FROM test_match_token_explicit WHERE matchToken(message, '^error$', 'array')
)
WHERE explain LIKE '%Name: idx_explicit%';
SELECT id FROM test_match_token_explicit WHERE matchToken(message, '^error$', 'array') ORDER BY id;

SELECT '-- explicit mismatched tokenizer: bypasses text index';
SELECT count() FROM (
    EXPLAIN indexes = 1
    SELECT * FROM test_match_token_explicit WHERE matchToken(message, '^error$', 'splitByNonAlpha')
)
WHERE explain LIKE '%Name: idx_explicit%';
SELECT id FROM test_match_token_explicit WHERE matchToken(message, '^error$', 'splitByNonAlpha') ORDER BY id;

DROP TABLE test_match_token_explicit;

-- ============================================================
-- §4 Prefix range-scan correctness (index on == index off)
--
-- Dictionary block size forced small (dictionary_block_size = 4) so a handful of
-- distinct tokens spans multiple blocks. Correctness is asserted by result
-- equivalence: each regexp runs twice -- text index on (range-scan path) and
-- off (brute-force scan) -- and the two groupArray(id) outputs must match.
--
-- A regexp exposes an anchored prefix to the index only when it starts with '^'
-- or a literal (no top-level alternation/leading '.*'). A bare substring or a
-- top-level alternation has no shared anchored prefix -> full scan.
-- ============================================================
DROP TABLE IF EXISTS test_rs_regexp;

CREATE TABLE test_rs_regexp
(
    id UInt32,
    message String,
    INDEX idx_msg(message) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 4)
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8;

INSERT INTO test_rs_regexp SELECT number, concat('t', toString(number), ' tail') FROM numbers(24);
INSERT INTO test_rs_regexp VALUES (100, 't\xff marker');

-- ============================================================
-- range-scan hit: anchored prefix regexp ^t1.*
-- ============================================================
SELECT '-- rs hit: ^t1.* (index on)';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, '^t1.*') ORDER BY id);
SELECT '-- rs hit: ^t1.* (index off)';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, '^t1.*') ORDER BY id);

-- ============================================================
-- non-anchored: t1.* matches tokens containing 't1'
-- ============================================================
SELECT '-- rs hit: t1.* (index on)';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't1.*') ORDER BY id);
SELECT '-- rs hit: t1.* (index off)';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't1.*') ORDER BY id);

-- ============================================================
-- no-prefix fallback: bare substring '1' has no anchored prefix -> full scan
-- Matches tokens containing '1': t1, t10..t19, t21.
-- ============================================================
SELECT '-- rs no-prefix: bare substring 1 (index on)';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, '1') ORDER BY id);
SELECT '-- rs no-prefix: bare substring 1 (index off)';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, '1') ORDER BY id);

-- ============================================================
-- no-prefix fallback: top-level alternation with no shared anchored prefix
-- 't1.*|t3.*' shares 't' but the analyzer treats top-level alternation as
-- prefix-less -> full scan. Result must still be correct.
-- ============================================================
SELECT '-- rs no-prefix: alternation t1.*|t3.* (index on)';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't1.*|t3.*') ORDER BY id);
SELECT '-- rs no-prefix: alternation t1.*|t3.* (index off)';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't1.*|t3.*') ORDER BY id);

-- ============================================================
-- coverage fallback: t.* matches all tokens -> full scan
-- ============================================================
SELECT '-- rs fallback: t.* (index on)';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't.*') ORDER BY id);
SELECT '-- rs fallback: t.* (index off)';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't.*') ORDER BY id);

-- ============================================================
-- prefixSuccessor boundary: prefix ending with 0xFF byte
-- 't\xff.*' must match token t\xff without looping or dropping.
-- ============================================================
SELECT '-- rs successor: t\xff.* (index on)';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't\xff.*') ORDER BY id);
SELECT '-- rs successor: t\xff.* (index off)';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_rs_regexp WHERE matchToken(message, 't\xff.*') ORDER BY id);

DROP TABLE test_rs_regexp;

-- ============================================================
-- dot-all semantics with text index: `.` must match newline (RE_DOT_NL).
-- The 'array' tokenizer keeps the whole element as one token, so 'a.b'
-- matches 'a\nb'. Index on and off must produce identical results.
-- ============================================================
DROP TABLE IF EXISTS test_dot_all;

SET enable_full_text_index = 1;

CREATE TABLE test_dot_all
(
    id UInt32,
    message String,
    INDEX idx_dot(message) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_dot_all VALUES (1, 'a\nb'), (2, 'abc'), (3, 'no match');

SELECT '-- dot-all index on: a.b matches a\\nb';
SET enable_full_text_index = 1;
SELECT groupArray(id) FROM (SELECT id FROM test_dot_all WHERE matchToken(message, 'a.b', 'array') ORDER BY id);
SELECT '-- dot-all index off: a.b matches a\\nb';
SET enable_full_text_index = 0;
SELECT groupArray(id) FROM (SELECT id FROM test_dot_all WHERE matchToken(message, 'a.b', 'array') ORDER BY id);

DROP TABLE test_dot_all;
