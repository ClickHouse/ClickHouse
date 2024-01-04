SELECT 'const arguments byteHammingDistance';
SELECT byteHammingDistance('abcd', 'abcd');
SELECT 'const arguments editDistance';
SELECT editDistance('clickhouse', 'mouse');

SELECT 'const arguments stringJaccardIndex';
SELECT stringJaccardIndex('clickhouse', 'mouse');

DROP TABLE if exists t;
CREATE TABLE t
(
	s1 String,
	s2 String
) ENGINE = MergeTree ORDER BY s1;

INSERT INTO t VALUES ('abcdefg', 'abcdef') ('abcdefg', 'bcdefg') ('abcdefg', '') ('mouse', 'clickhouse');
SELECT 'byteHammingDistance';
SELECT byteHammingDistance(s1, s2) FROM t ORDER BY s1, s2;
SELECT 'byteHammingDistance(const, non const)';
SELECT byteHammingDistance('abc', s2) FROM t ORDER BY s1, s2;
SELECT 'byteHammingDistance(non const, const)';
SELECT byteHammingDistance(s2, 'def') FROM t ORDER BY s1, s2;

SELECT 'mismatches(alias)';
SELECT mismatches(s1, s2) FROM t ORDER BY s1, s2;
SELECT mismatches('abc', s2) FROM t ORDER BY s1, s2;
SELECT mismatches(s2, 'def') FROM t ORDER BY s1, s2;

SELECT 'stringJaccardIndex';
SELECT stringJaccardIndex(s1, s2) FROM t ORDER BY s1, s2;
SELECT stringJaccardIndexUTF8(s1, s2) FROM t ORDER BY s1, s2;

-- we do not perform full UTF8 validation, so sometimes it just returns some result
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

SELECT 'editDistance';
SELECT editDistance(s1, s2) FROM t ORDER BY s1, s2;
SELECT 'levenshteinDistance';
SELECT levenshteinDistance(s1, s2) FROM t ORDER BY s1, s2;

SELECT editDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}

DROP TABLE t;
