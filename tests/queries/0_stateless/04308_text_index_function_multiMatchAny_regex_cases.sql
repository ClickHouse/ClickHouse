-- Tags: no-fasttest, no-parallel-replicas, use-vectorscan

-- Tests that multiMatchAny() over a text-indexed column returns the same rows with and without the
-- index, across the adversarial pattern cases from 00926_multimatch.sql (empty needles, regex
-- metacharacters, character classes, anchors, null bytes, Unicode, needles shorter than the ngram
-- size, ...). The text index analyzes each pattern and prunes granules, but for patterns it cannot
-- reduce to a token requirement (e.g. '.*', 'a.bc', '[ae]' classes) it must fall back to a full scan
-- without dropping any matching row.
--
-- Regex search is intended to be used with the ngrams (or sparseGrams) tokenizer: the literal runs
-- extracted from each pattern are decomposed into ngrams, so the index can prune on substrings that
-- need not align with whole-word boundaries.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX inv_idx(str) TYPE text(tokenizer = ngrams(3), dictionary_block_size = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (1,  'mpnsguhwsitzvuleiwebwjfitmsg'),
    (2,  'khljxzxlpcrxpkrfybbfk'),
    (3,  'mznihnmshftvnmmhnrulizzpslq'),
    (4,  'ololo'),
    (5,  'abc'),
    (6,  'abcd'),
    (7,  'abcdef'),
    (8,  'aaaa'),
    (9,  'фабрикант'),
    (10, '/odezhda-dlya-bega/'),
    (11, 'a\nbc'),
    (12, 'a\0bc'),
    (13, 'vladizlvav dabe don\'t heart me no more'),
    (14, 'gogleuedeuniangoogle');

-- Each case prints the ids matched using the index, then 1 if that set equals the brute-force result
-- (use_skip_indexes = 0). All equality flags must be 1. groupArray is wrapped in arraySort because
-- the read order across granules is not deterministic.

SELECT '-- literal substrings present in exactly one row';
WITH ['lpc', 'rxpkrfybb', 'crxp'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['nrul', 'mshftvnmmhnr', 'mhnrulizzps'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- needles that match nothing';
WITH ['wbirxqoabpblrnvvmjizj', 'cfcxhuvrexyzyjsh', 'bumoozxdkjglzu'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- empty needle is always true and disables pruning';
WITH [''] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['', 'abc'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- short needles (shorter than the ngram size) disable pruning but stay correct';
WITH ['k'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['a', 'cd'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- catch-all regexes disable pruning';
WITH ['.*'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['...'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- regex metacharacters: any char, optional, fixed-length runs';
WITH ['a.bc'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['a?bc'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['a.....'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['a......'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['a......', 'a.....'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- character classes and anchors on a Unicode haystack';
WITH ['f[ae]b[ei]rl', 'ф[иаэе]б[еэи][рпл]', 'афиукд', '^ф[аиеэ]?б?[еэи]?$', 'берлик', 'fab', 'фа[беьв]+е?[рлко]'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- patterns that must not match the aaaa haystack';
WITH ['.*aa.*aaa.*', 'aaaaaa{2}', '(aa){3}'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- a needle containing a null byte (the pattern is truncated at the null, so it matches any a)';
WITH ['a\0d'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- url-like literal needle';
WITH ['/odezhda-dlya-bega/', 'kurtki-i-vetrovki-dlya-bega'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- substrings spanning apparent word boundaries (the ngrams tokenizer advantage)';
WITH ['google', 'unian1'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

WITH ['no mo??', 'dont', 'h.rt me'] AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- an empty needle array never matches';
WITH []::Array(String) AS needles
SELECT (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 1) AS ids,
       ids = (SELECT arraySort(groupArray(id)) FROM tab WHERE multiMatchAny(str, needles) SETTINGS use_skip_indexes = 0);

SELECT '-- literal needles prune granules, while catch-all / too-short / class-only patterns do not';
SELECT trim(explain)
FROM (EXPLAIN PLAN indexes = 1 SELECT id FROM tab WHERE multiMatchAny(str, ['lpc', 'rxpkrfybb', 'crxp']))
WHERE explain LIKE '%Granules: %';

SELECT trim(explain)
FROM (EXPLAIN PLAN indexes = 1 SELECT id FROM tab WHERE multiMatchAny(str, ['/odezhda-dlya-bega/']))
WHERE explain LIKE '%Granules: %';

SELECT trim(explain)
FROM (EXPLAIN PLAN indexes = 1 SELECT id FROM tab WHERE multiMatchAny(str, ['OLAP', '.*']))
WHERE explain LIKE '%Granules: %';

SELECT trim(explain)
FROM (EXPLAIN PLAN indexes = 1 SELECT id FROM tab WHERE multiMatchAny(str, ['k']))
WHERE explain LIKE '%Granules: %';

SELECT '-- force_data_skipping_indices accepts literal needles but rejects a catch-all';
SELECT count() FROM tab WHERE multiMatchAny(str, ['lpc', 'google']) SETTINGS force_data_skipping_indices = 'inv_idx';
SELECT count() FROM tab WHERE multiMatchAny(str, ['lpc', '.*']) SETTINGS force_data_skipping_indices = 'inv_idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab;
