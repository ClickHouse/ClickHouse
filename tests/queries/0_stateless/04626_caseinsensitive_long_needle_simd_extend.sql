-- Test case-insensitive ASCII search with a needle longer than the SIMD register,
-- exercising the match-extension loop past the cached bytes.
DROP TABLE IF EXISTS t_ci_long;
CREATE TABLE t_ci_long (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ci_long VALUES
  ('some padding text that is not a match at all here'),
  ('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCD'),
  ('prefix_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCD_suffix'),
  ('almost ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCX no'),
  ('short');

SELECT s, positionCaseInsensitive(s, 'abcdefghijklmnopqrstuvwxyz0123456789abcd') AS p
FROM t_ci_long ORDER BY s;

SELECT '-- countSubstringsCaseInsensitive';
SELECT s, countSubstringsCaseInsensitive(s, 'abcdefghijklmnopqrstuvwxyz0123456789abcd') AS c
FROM t_ci_long ORDER BY s;

SELECT '-- multiSearchAnyCaseInsensitive';
SELECT s, multiSearchAnyCaseInsensitive(s, ['abcdefghijklmnopqrstuvwxyz0123456789abcd']) AS m
FROM t_ci_long ORDER BY s;

DROP TABLE t_ci_long;
