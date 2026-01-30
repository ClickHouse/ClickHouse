SELECT '-- const arguments';
-- just to see it works
SELECT 'clickhouse' AS s1, 'mouse' AS s2, byteHammingDistance(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, editDistance(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, damerauLevenshteinDistance(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, stringJaccardIndex(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, stringJaccardIndexUTF8(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, jaroSimilarity(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, jaroWinklerSimilarity(s1, s2);

SELECT '-- test aliases';
SELECT 'clickhouse' AS s1, 'mouse' AS s2, mismatches(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, levenshteinDistance(s1, s2);

SELECT '-- Deny DoS using too large inputs';
SELECT editDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}
SELECT damerauLevenshteinDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}
SELECT jaroSimilarity(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}
SELECT jaroWinklerSimilarity(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    s1 String,
    s2 String
) ENGINE = MergeTree ORDER BY s1;

-- actual test cases
INSERT INTO t VALUES ('', '') ('abc', '') ('', 'abc') ('abc', 'abc') ('abc', 'ab') ('abc', 'bc') ('clickhouse', 'mouse') ('我是谁', 'Tom') ('Jerry', '我是谁') ('我是谁', '我是我');

SELECT '-- non-const arguments';
SELECT 'byteHammingDistance', s1, s2, byteHammingDistance(s1, s2) FROM t ORDER BY ALL;
SELECT 'editDistance', s1, s2, editDistance(s1, s2) FROM t ORDER BY ALL;
SELECT 'editDistanceUTF8', s1, s2, editDistanceUTF8(s1, s2) FROM t ORDER BY ALL;
SELECT 'damerauLevenshteinDistance', s1, s2, damerauLevenshteinDistance(s1, s2) FROM t ORDER BY ALL;
SELECT 'stringJaccardIndex', s1, s2, stringJaccardIndex(s1, s2) FROM t ORDER BY ALL;
SELECT 'stringJaccardIndexUTF8', s1, s2, stringJaccardIndexUTF8(s1, s2) FROM t ORDER BY ALL;
SELECT 'jaroSimilarity', s1, s2, jaroSimilarity(s1, s2) FROM t ORDER BY ALL;
SELECT 'jaroWinklerSimilarity', s1, s2, jaroWinklerSimilarity(s1, s2) FROM t ORDER BY ALL;

SELECT '-- Special UTF-8 tests';
-- We do not perform full UTF8 validation, so sometimes it just returns some result
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\x48\x65\x6C'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xFF\xFF\xFF\xFF'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\x41\xE2\x82\xAC'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xF0\x9F\x99\x82'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xFF'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC2\x01')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC1\x81')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xF0\x80\x80\x41')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC0\x80')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xD8\x00 ')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xDC\x00')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8('😃🌍', '🙃😃🌑'), stringJaccardIndex('😃🌍', '🙃😃🌑');

DROP TABLE t;
