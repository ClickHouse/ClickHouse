-- Test case-insensitive ASCII search with a needle longer than the SIMD register,
-- exercising the match-extension loop past the cached bytes.
-- The tail past the first SIMD chunk (bytes 36-39) is mixed-case on BOTH the
-- haystack ('AbCd') and needle ('aBcD') sides with the casing flipped per byte,
-- so the extension loop must fold both operands: dropping lowerASCII on either
-- the haystack (StringSearcher.h:216/269/318) or the needle side turns a match
-- into a miss for at least one byte.
DROP TABLE IF EXISTS t_ci_long;
CREATE TABLE t_ci_long (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ci_long VALUES
  ('some padding text that is not a match at all here'),
  ('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789AbCd'),
  ('prefix_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789AbCd_suffix'),
  ('almost ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789AbCX no'),
  ('short');

SELECT s, positionCaseInsensitive(s, 'abcdefghijklmnopqrstuvwxyz0123456789aBcD') AS p
FROM t_ci_long ORDER BY s;

SELECT '-- countSubstringsCaseInsensitive';
SELECT s, countSubstringsCaseInsensitive(s, 'abcdefghijklmnopqrstuvwxyz0123456789aBcD') AS c
FROM t_ci_long ORDER BY s;

SELECT '-- multiSearchAnyCaseInsensitive';
SELECT s, multiSearchAnyCaseInsensitive(s, ['abcdefghijklmnopqrstuvwxyz0123456789aBcD']) AS m
FROM t_ci_long ORDER BY s;

DROP TABLE t_ci_long;
