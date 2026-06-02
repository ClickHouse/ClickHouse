-- Tags: no-fasttest
-- Aho-Corasick (daachorse) path must preserve the legacy multiSearchAny* contract:
--   * an empty needle matches every haystack (result 1)
--   * duplicate needles are accepted (daachorse rejects them, so we de-duplicate)
--   * the case-insensitive ASCII variant folds ASCII only (no Unicode folding),
--     while the *UTF8 variant folds Unicode.

SET send_logs_level = 'fatal';

SELECT '-- forced daachorse path (<= 255 needles) --';

-- empty needle -> matches everything
SELECT multiSearchAny('abc', ['']) SETTINGS force_daachorse_for_multi_search = 1;
SELECT multiSearchAny('abc', ['x', '', 'z']) SETTINGS force_daachorse_for_multi_search = 1;
SELECT multiSearchAnyCaseInsensitive('abc', ['']) SETTINGS force_daachorse_for_multi_search = 1;

-- duplicate needles are accepted
SELECT multiSearchAny('abc', ['a', 'a', 'b']) SETTINGS force_daachorse_for_multi_search = 1;
SELECT multiSearchAny('xyz', ['a', 'a', 'b']) SETTINGS force_daachorse_for_multi_search = 1;
-- duplicates that collide only after case folding
SELECT multiSearchAnyCaseInsensitive('abc', ['ABC', 'abc']) SETTINGS force_daachorse_for_multi_search = 1;

-- ASCII case-insensitive folds ASCII only: über must NOT match ÜBER
SELECT multiSearchAnyCaseInsensitive('über', ['ÜBER']) SETTINGS force_daachorse_for_multi_search = 1;
SELECT multiSearchAnyCaseInsensitive('HELLO world', ['hello']) SETTINGS force_daachorse_for_multi_search = 1;
-- UTF8 case-insensitive folds Unicode: über matches ÜBER
SELECT multiSearchAnyCaseInsensitiveUTF8('über', ['ÜBER']) SETTINGS force_daachorse_for_multi_search = 1;

SELECT '-- automatic daachorse path (> 255 needles) --';

WITH arrayMap(x -> 'pattern_' || toString(x), range(300)) AS pats
SELECT
    multiSearchAny('has pattern_150 here', pats),
    multiSearchAny('nothing relevant', pats),
    multiSearchAny('nothing relevant', arrayPushFront(pats, '')),
    multiSearchAny('has pattern_7 here', arrayConcat(pats, pats));

-- case-folding mode must be correct on the automatic path too (haystack matches only via folding)
WITH arrayMap(x -> 'zzz' || toString(x), range(300)) AS pats
SELECT
    multiSearchAnyCaseInsensitive('über', arrayPushFront(pats, 'ÜBER')),
    multiSearchAnyCaseInsensitiveUTF8('über', arrayPushFront(pats, 'ÜBER'));
