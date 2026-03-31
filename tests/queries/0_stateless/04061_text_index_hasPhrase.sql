-- Tags: no-fasttest, no-parallel-replicas

-- Ensure text index is always used (use_skip_indexes_on_data_read=0 has a known
-- incompatibility with text index direct read and can cause hangs).
SET use_skip_indexes_on_data_read = 1;

-- ============================================================
-- Section 1: Pure function correctness (no index)
-- ============================================================

SELECT '-- Section 1: Pure function correctness';

-- Basic 2-token phrase
SELECT hasPhrase('the quick brown fox', 'quick brown');
-- Basic 3-token phrase
SELECT hasPhrase('the quick brown fox', 'quick brown fox');
-- Full match
SELECT hasPhrase('the quick brown fox', 'the quick brown fox');
-- Single token (equivalent to hasToken)
SELECT hasPhrase('the quick brown fox', 'quick');
-- Empty phrase -> always 1
SELECT hasPhrase('the quick brown fox', '');
-- Tokens present but not adjacent -> 0
SELECT hasPhrase('the quick brown fox', 'quick fox');
-- Tokens present but wrong order -> 0
SELECT hasPhrase('the quick brown fox', 'brown quick');
-- Phrase not in text at all
SELECT hasPhrase('the quick brown fox', 'lazy dog');
-- Single word not present
SELECT hasPhrase('the quick brown fox', 'cat');
-- Repeated tokens in phrase
SELECT hasPhrase('to be or not to be that is the question', 'to be');
SELECT hasPhrase('to be or not to be that is the question', 'not to be');
-- Phrase at start
SELECT hasPhrase('hello world foo bar', 'hello world');
-- Phrase at end
SELECT hasPhrase('hello world foo bar', 'foo bar');

SELECT '-- Section 1b: Edge cases';

-- Empty haystack
SELECT hasPhrase('', 'hello');
SELECT hasPhrase('', '');
-- Special characters as separators
SELECT hasPhrase('error: segmentation fault (core dumped)', 'segmentation fault');
SELECT hasPhrase('path/to/file.txt contains data', 'to file');
-- Numeric tokens
SELECT hasPhrase('error 404 not found', '404 not');
SELECT hasPhrase('version 1 2 3', '1 2 3');
-- Long text
SELECT hasPhrase(
    'Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua',
    'sed do eiusmod'
);
-- Case sensitive
SELECT hasPhrase('The Quick Brown Fox', 'the quick');
SELECT hasPhrase('The Quick Brown Fox', 'The Quick');

SELECT '-- Section 1c: Explicit tokenizer';

SELECT hasPhrase('hello world test', 'hello world', 'splitByNonAlpha');
SELECT hasPhrase('hello world test', 'world test', 'splitByNonAlpha');

-- ============================================================
-- Section 2: Text index integration (with text index)
-- ============================================================

SELECT '-- Section 2: Text index integration';

DROP TABLE IF EXISTS test_phrase_idx;

CREATE TABLE test_phrase_idx
(
    id UInt64,
    content String,
    INDEX idx content TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO test_phrase_idx VALUES
    (1, 'the quick brown fox'),
    (2, 'jumps over the lazy dog'),
    (3, 'quick fox is not brown fox'),
    (4, 'brown quick reverse order'),
    (5, 'segmentation fault core dumped'),
    (6, 'no fault here at all'),
    (7, 'to be or not to be'),
    (8, 'that is the question');

-- Basic phrase query
SELECT '-- Basic phrase query';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'quick brown') ORDER BY id;

-- 3-token phrase
SELECT '-- 3-token phrase';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'quick brown fox') ORDER BY id;

-- Tokens not adjacent in row 1 (quick _ _ fox), but adjacent in row 3 (quick fox ...)
SELECT '-- Phrase: quick fox';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'quick fox') ORDER BY id;

-- 'brown quick' appears in row 4 as adjacent tokens in that order
SELECT '-- Phrase: brown quick';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'brown quick') ORDER BY id;

-- Single token phrase
SELECT '-- Single token';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'fault') ORDER BY id;

-- Empty phrase matches all
SELECT '-- Empty phrase';
SELECT count() FROM test_phrase_idx WHERE hasPhrase(content, '');

-- Phrase not present at all
SELECT '-- Phrase not present';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'hello world') ORDER BY id;

-- Repeated tokens
SELECT '-- Repeated tokens: to be';
SELECT id, content FROM test_phrase_idx WHERE hasPhrase(content, 'to be') ORDER BY id;

-- ============================================================
-- Section 3: EXPLAIN indexes=1 verification
-- ============================================================

SELECT '-- Section 3: EXPLAIN indexes verification';

-- Verify that hasPhrase uses skip index (Name: idx should appear in EXPLAIN output)
SELECT '-- Skip index name in EXPLAIN';
SELECT count() > 0
FROM (
    EXPLAIN indexes = 1 SELECT * FROM test_phrase_idx WHERE hasPhrase(content, 'quick brown')
)
WHERE explain LIKE '%Name:%idx%';

-- Non-matching phrase should skip all parts (Parts: 0/1)
SELECT '-- Non-matching phrase skips parts';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM test_phrase_idx WHERE hasPhrase(content, 'nonexistent phrase here')
)
WHERE explain LIKE '%Parts: 0/1%';

-- ============================================================
-- Section 4: Consistency between indexed and non-indexed tables
-- ============================================================

SELECT '-- Section 4: Index vs no-index consistency';

DROP TABLE IF EXISTS test_phrase_noidx;

CREATE TABLE test_phrase_noidx
(
    id UInt64,
    content String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_phrase_noidx VALUES
    (1, 'the quick brown fox'),
    (2, 'jumps over the lazy dog'),
    (3, 'quick fox is not brown fox'),
    (4, 'brown quick reverse order'),
    (5, 'segmentation fault core dumped'),
    (6, 'no fault here at all'),
    (7, 'to be or not to be'),
    (8, 'that is the question');

-- Compare results: indexed vs non-indexed should be identical
SELECT '-- Consistency: quick brown';
SELECT id FROM test_phrase_idx WHERE hasPhrase(content, 'quick brown') ORDER BY id;
SELECT id FROM test_phrase_noidx WHERE hasPhrase(content, 'quick brown') ORDER BY id;

SELECT '-- Consistency: segmentation fault';
SELECT id FROM test_phrase_idx WHERE hasPhrase(content, 'segmentation fault') ORDER BY id;
SELECT id FROM test_phrase_noidx WHERE hasPhrase(content, 'segmentation fault') ORDER BY id;

SELECT '-- Consistency: to be';
SELECT id FROM test_phrase_idx WHERE hasPhrase(content, 'to be') ORDER BY id;
SELECT id FROM test_phrase_noidx WHERE hasPhrase(content, 'to be') ORDER BY id;

SELECT '-- Consistency: not present';
SELECT id FROM test_phrase_idx WHERE hasPhrase(content, 'hello world') ORDER BY id;
SELECT id FROM test_phrase_noidx WHERE hasPhrase(content, 'hello world') ORDER BY id;

SELECT '-- Consistency: empty phrase';
SELECT count() FROM test_phrase_idx WHERE hasPhrase(content, '');
SELECT count() FROM test_phrase_noidx WHERE hasPhrase(content, '');

DROP TABLE IF EXISTS test_phrase_idx;
DROP TABLE IF EXISTS test_phrase_noidx;

-- ============================================================
-- Section 5: Ngram tokenizer rejection (FC-5)
-- ============================================================

SELECT '-- Section 5: Ngram tokenizer rejection';

-- hasPhrase should reject ngram tokenizer with an exception
SELECT hasPhrase('hello world', 'hello world', 'ngram(3)'); -- { serverError BAD_ARGUMENTS }

-- ============================================================
-- Section 6: OPTIMIZE TABLE merge correctness (FC-3)
-- ============================================================

SELECT '-- Section 6: Merge correctness';

DROP TABLE IF EXISTS test_phrase_merge;

CREATE TABLE test_phrase_merge
(
    id UInt64,
    content String,
    INDEX idx content TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

-- Insert multiple batches to create multiple parts
INSERT INTO test_phrase_merge VALUES (1, 'the quick brown fox'), (2, 'jumps over the lazy dog');
INSERT INTO test_phrase_merge VALUES (3, 'quick fox is not brown fox'), (4, 'brown quick reverse order');
INSERT INTO test_phrase_merge VALUES (5, 'to be or not to be'), (6, 'that is the question');

-- Query before merge
SELECT '-- Before merge';
SELECT id FROM test_phrase_merge WHERE hasPhrase(content, 'quick brown') ORDER BY id;
SELECT id FROM test_phrase_merge WHERE hasPhrase(content, 'to be') ORDER BY id;

OPTIMIZE TABLE test_phrase_merge FINAL;

-- Query after merge — results must be identical
SELECT '-- After merge';
SELECT id FROM test_phrase_merge WHERE hasPhrase(content, 'quick brown') ORDER BY id;
SELECT id FROM test_phrase_merge WHERE hasPhrase(content, 'to be') ORDER BY id;

DROP TABLE IF EXISTS test_phrase_merge;

-- ============================================================
-- Section 7: Multiple tokenizers (FC-4)
-- ============================================================

SELECT '-- Section 7: Multiple tokenizers';

-- Test with splitByString tokenizer
DROP TABLE IF EXISTS test_phrase_split;

CREATE TABLE test_phrase_split
(
    id UInt64,
    content String,
    INDEX idx content TYPE text(tokenizer = 'splitByString') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO test_phrase_split VALUES
    (1, 'the quick brown fox'),
    (2, 'quick fox is not brown'),
    (3, 'brown quick reverse');

SELECT '-- splitByString tokenizer: quick brown';
SELECT id FROM test_phrase_split WHERE hasPhrase(content, 'quick brown') ORDER BY id;

DROP TABLE IF EXISTS test_phrase_split;
