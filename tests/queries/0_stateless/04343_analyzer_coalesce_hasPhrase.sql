-- Verifies the CoalesceHasPhrasePass rewrite of hasPhrase OR/AND chains into hasAnyPhrases/hasAllPhrases.
-- OR coalescing is on by default (optimize_rewrite_has_phrase_or_chain); AND coalescing is off by
-- default (optimize_rewrite_has_phrase_and_chain) because AND short-circuits and coalescing it can regress.
SET enable_analyzer = 1;

SELECT '-- OR chain over the same column -> hasAnyPhrases';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('the quick brown fox jumps over the lazy dog') AS s
WHERE hasPhrase(s, 'quick brown') OR hasPhrase(s, 'lazy dog') OR hasPhrase(s, 'fox jumps')
SETTINGS optimize_rewrite_has_phrase_or_chain = 1;

SELECT '-- AND chain is NOT coalesced by default';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('the quick brown fox jumps over the lazy dog') AS s
WHERE hasPhrase(s, 'quick brown') AND hasPhrase(s, 'lazy dog')
SETTINGS optimize_rewrite_has_phrase_or_chain = 1;

SELECT '-- AND chain -> hasAllPhrases when explicitly enabled (single folded arg is padded with AND 1)';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('the quick brown fox jumps over the lazy dog') AS s
WHERE hasPhrase(s, 'quick brown') AND hasPhrase(s, 'lazy dog')
SETTINGS optimize_rewrite_has_phrase_and_chain = 1;

SELECT '-- OR setting disabled: no rewrite';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('the quick brown fox jumps over the lazy dog') AS s
WHERE hasPhrase(s, 'quick brown') OR hasPhrase(s, 'lazy dog')
SETTINGS optimize_rewrite_has_phrase_or_chain = 0;

SELECT '-- non-hasPhrase operands are preserved';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('the quick brown fox') AS s
WHERE hasPhrase(s, 'quick brown') OR hasPhrase(s, 'brown fox') OR s = 'x'
SETTINGS optimize_rewrite_has_phrase_or_chain = 1;

SELECT '-- different columns are NOT coalesced';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('the quick brown fox') AS s1, materialize('the lazy dog') AS s2
WHERE hasPhrase(s1, 'quick brown') OR hasPhrase(s2, 'lazy dog')
SETTINGS optimize_rewrite_has_phrase_or_chain = 1;

SELECT '-- different tokenizers are NOT coalesced (no-arg vs explicit, and distinct explicit)';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('abcdef') AS s
WHERE hasPhrase(s, 'abc') OR hasPhrase(s, 'bcd', 'ngrams(3)')
SETTINGS optimize_rewrite_has_phrase_or_chain = 1;

SELECT '-- same explicit tokenizer IS coalesced (3-arg hasAnyPhrases)';
EXPLAIN QUERY TREE run_passes=1
SELECT materialize('abcdef') AS s
WHERE hasPhrase(s, 'abc', 'ngrams(3)') OR hasPhrase(s, 'cde', 'ngrams(3)')
SETTINGS optimize_rewrite_has_phrase_or_chain = 1;

SELECT '-- functional: WHERE result is identical with the rewrite on and off';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'the quick brown fox jumps over the lazy dog'),
    (2, 'a fast red car drove past the old house'),
    (3, 'clickhouse is a fast analytical database'),
    (4, 'the brown quick fox'),
    (5, 'quick brown foxes are fast');

SELECT id FROM tab WHERE hasPhrase(message, 'quick brown') OR hasPhrase(message, 'old house')
ORDER BY id SETTINGS optimize_rewrite_has_phrase_or_chain = 1;
SELECT id FROM tab WHERE hasPhrase(message, 'quick brown') OR hasPhrase(message, 'old house')
ORDER BY id SETTINGS optimize_rewrite_has_phrase_or_chain = 0;

SELECT id FROM tab WHERE hasPhrase(message, 'quick brown') AND hasPhrase(message, 'lazy dog')
ORDER BY id SETTINGS optimize_rewrite_has_phrase_and_chain = 1;
SELECT id FROM tab WHERE hasPhrase(message, 'quick brown') AND hasPhrase(message, 'lazy dog')
ORDER BY id SETTINGS optimize_rewrite_has_phrase_and_chain = 0;

DROP TABLE tab;
