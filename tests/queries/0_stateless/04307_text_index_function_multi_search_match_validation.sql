-- Tags: no-fasttest, use-vectorscan

-- Tests that the text index runs the same argument validation as `multiSearchAny` and `multiMatchAny` before it
-- prunes granules. The skip-index path can decide a predicate is false without executing the real function, but the
-- functions reject some constant arguments (more than 255 needles for `multiSearchAny`; `allow_hyperscan`, the regexp
-- length limits, the expensive-regexp rejection and any pattern Hyperscan fails to compile for `multiMatchAny`)
-- before scanning. Without the shared validation, an invalid query could prune all granules and return 0 instead of
-- raising the exception the function itself would raise. Every invalid case below must throw the same exception with
-- and without the index.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX idx str TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar baz');

SELECT '-- multiSearchAny with more than 255 needles raises an exception, with and without the index';
SELECT count() FROM tab WHERE multiSearchAny(str, arrayMap(x -> 'abc' || toString(x), range(256))) SETTINGS use_skip_indexes = 0; -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
SELECT count() FROM tab WHERE multiSearchAny(str, arrayMap(x -> 'abc' || toString(x), range(256))) SETTINGS use_skip_indexes = 1; -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

SELECT '-- 255 needles is accepted by the index';
SELECT count() FROM tab WHERE multiSearchAny(str, arrayMap(x -> 'abc' || toString(x), range(255))) SETTINGS use_skip_indexes = 1;

SELECT '-- valid multiSearchAny uses the index and returns the matching rows';
SELECT id FROM tab WHERE multiSearchAny(str, ['bar', 'qux']) ORDER BY id SETTINGS use_skip_indexes = 1;

SELECT '-- multiMatchAny with allow_hyperscan = 0 raises an exception, with and without the index';
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello']) SETTINGS allow_hyperscan = 0, use_skip_indexes = 0; -- { serverError FUNCTION_NOT_ALLOWED }
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello']) SETTINGS allow_hyperscan = 0, use_skip_indexes = 1; -- { serverError FUNCTION_NOT_ALLOWED }

SELECT '-- multiMatchAny with a regexp longer than max_hyperscan_regexp_length raises an exception, with and without the index';
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello']) SETTINGS max_hyperscan_regexp_length = 3, use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello']) SETTINGS max_hyperscan_regexp_length = 3, use_skip_indexes = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- multiMatchAny with an expensive regexp raises an exception when reject_expensive_hyperscan_regexps = 1, with and without the index';
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello.{51}']) SETTINGS use_skip_indexes = 0; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello.{51}']) SETTINGS use_skip_indexes = 1; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

SELECT '-- the same expensive regexp is accepted when reject_expensive_hyperscan_regexps = 0';
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello.{51}']) SETTINGS reject_expensive_hyperscan_regexps = 0, use_skip_indexes = 1;

-- The settings checks above accept `abc[`, but Hyperscan rejects it when it compiles the pattern set. `abc[` analyzes
-- to the token `abc`, which is absent from the data, so without compiling the pattern set the index would prune every
-- granule and return 0 instead of raising the exception that the function raises.
SELECT '-- multiMatchAny with a regexp Hyperscan cannot compile raises an exception, with and without the index';
SELECT count() FROM tab WHERE multiMatchAny(str, ['abc[']) SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE multiMatchAny(str, ['abc[']) SETTINGS use_skip_indexes = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- the same applies when only one needle in the set is malformed';
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello', 'abc[']) SETTINGS use_skip_indexes = 0; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE multiMatchAny(str, ['hello', 'abc[']) SETTINGS use_skip_indexes = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- valid multiMatchAny uses the index and returns the matching rows';
SELECT id FROM tab WHERE multiMatchAny(str, ['hello', 'qux']) ORDER BY id SETTINGS use_skip_indexes = 1;

DROP TABLE tab;
