-- Tags: no-fasttest
-- Test Aho-Corasick multi-pattern search: automatic fallback for >255 patterns

SET send_logs_level = 'fatal';

-- ============================================================================
-- EDGE CASE TESTS
-- ============================================================================

-- Empty pattern array is not supported (Array(Nothing) type error), skipping that test

-- ============================================================================
-- AUTOMATIC FALLBACK TESTS (triggers Aho-Corasick for >255 patterns)
-- ============================================================================

-- Generate 300 patterns and test matching
-- Pattern format: 'pattern_N' where N is 0-299
SELECT multiSearchAny('this string contains pattern_150 somewhere',
    arrayMap(x -> concat('pattern_', toString(x)), range(300))) AS result;

-- Test case-insensitive variant with >255 patterns
SELECT multiSearchAnyCaseInsensitive('THIS STRING CONTAINS PATTERN_200 SOMEWHERE',
    arrayMap(x -> concat('pattern_', toString(x)), range(300))) AS result_ci;

-- Test UTF8 variant
SELECT multiSearchAnyUTF8('unicode text with pattern_50 and émojis',
    arrayMap(x -> concat('pattern_', toString(x)), range(300))) AS utf8_test;

-- Test case-insensitive UTF8 variant
SELECT multiSearchAnyCaseInsensitiveUTF8('UNICODE TEXT WITH PATTERN_75 AND ÉMOJIS',
    arrayMap(x -> concat('pattern_', toString(x)), range(300))) AS utf8_ci_test;

-- Test no match case
SELECT multiSearchAny('no matches here',
    arrayMap(x -> concat('pattern_', toString(x)), range(300))) AS no_match;

-- Test with exactly 256 patterns (boundary)
SELECT multiSearchAny('found pattern_100 here',
    arrayMap(x -> concat('pattern_', toString(x)), range(256))) AS boundary_test;

-- Test with 1000 patterns
SELECT multiSearchAny('looking for pattern_999 in text',
    arrayMap(x -> concat('pattern_', toString(x)), range(1000))) AS thousand_patterns;

-- Test with 100000 patterns
SELECT multiSearchAny('looking for pattern_99999 in text',
    arrayMap(x -> concat('pattern_', toString(x)), range(100000))) AS hundred_thousand_patterns;

-- Test with table data
DROP TABLE IF EXISTS test_aho_corasick;
CREATE TABLE test_aho_corasick (id UInt32, text String) ENGINE = Memory;
INSERT INTO test_aho_corasick VALUES (1, 'contains pattern_42'), (2, 'no match'), (3, 'has pattern_256');

SELECT id, multiSearchAny(text, arrayMap(x -> concat('pattern_', toString(x)), range(300))) AS matched
FROM test_aho_corasick
ORDER BY id;

DROP TABLE test_aho_corasick;
