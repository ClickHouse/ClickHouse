-- Test LIKE/ILIKE/NOT LIKE/NOT ILIKE with constant haystack and non-constant needle.
-- See https://github.com/ClickHouse/ClickHouse/issues/84187

-- Basic LIKE with constant haystack
SELECT 'foo_bar' LIKE pattern FROM (SELECT arrayJoin(['%bar', 'foo%', '%baz%', '%o_b%']) AS pattern);

-- ILIKE with constant haystack
SELECT 'FooBar' ILIKE pattern FROM (SELECT arrayJoin(['%bar', '%BAR', 'foo%', '%baz%']) AS pattern);

-- NOT LIKE
SELECT 'foo_bar' NOT LIKE pattern FROM (SELECT arrayJoin(['%bar', 'foo%', '%baz%']) AS pattern);

-- NOT ILIKE
SELECT 'FooBar' NOT ILIKE pattern FROM (SELECT arrayJoin(['%bar', '%BAR', '%baz%']) AS pattern);

-- match() with constant haystack
SELECT match('foo_bar', pattern) FROM (SELECT arrayJoin(['^foo', '.*bar$', '^baz']) AS pattern);

-- From table column
DROP TABLE IF EXISTS test_like_const;
CREATE TABLE test_like_const (pattern String) ENGINE = Memory;
INSERT INTO test_like_const VALUES ('%bar'), ('foo%'), ('%baz%');
SELECT 'foo_bar' LIKE pattern FROM test_like_const;
DROP TABLE test_like_const;

-- Edge cases
SELECT '' LIKE pattern FROM (SELECT arrayJoin(['%', '%%']) AS pattern);
SELECT 'x' LIKE pattern FROM (SELECT arrayJoin(['%', '_', 'x', 'y']) AS pattern);

-- Column LIKE column (vectorVector / vectorFixedVector): the trivial `%` / `%%`
-- (and `.*` / `.*?` for `match`) shortcut must apply regardless of argument constness,
-- otherwise `'' LIKE '%%'` would return 0 here but 1 in the constant-haystack direction.
DROP TABLE IF EXISTS test_like_vv;
CREATE TABLE test_like_vv (haystack String, pattern String) ENGINE = Memory;
INSERT INTO test_like_vv VALUES ('', '%'), ('', '%%'), ('x', '%'), ('x', '%%'), ('foo', '%bar'), ('foo', '%oo%');
SELECT haystack, pattern, haystack LIKE pattern, haystack NOT LIKE pattern, match(haystack, pattern) FROM test_like_vv ORDER BY haystack, pattern;
DROP TABLE test_like_vv;

DROP TABLE IF EXISTS test_like_fvv;
CREATE TABLE test_like_fvv (haystack FixedString(2), pattern String) ENGINE = Memory;
INSERT INTO test_like_fvv VALUES ('aa', '%'), ('aa', '%%'), ('bb', '%'), ('bb', '%%');
SELECT haystack, pattern, haystack LIKE pattern, haystack NOT LIKE pattern, match(haystack, pattern) FROM test_like_fvv ORDER BY haystack, pattern;
DROP TABLE test_like_fvv;

-- `match` trivial pattern shortcut across all argument shapes (const-const, vec-const, const-vec, vec-vec):
SELECT match('', '.*'), match('', '.*?');
SELECT match(haystack, '.*'), match(haystack, '.*?') FROM (SELECT arrayJoin(['', 'x']) AS haystack) ORDER BY haystack;
SELECT match('', pattern) FROM (SELECT arrayJoin(['.*', '.*?']) AS pattern);
DROP TABLE IF EXISTS test_match_vv;
CREATE TABLE test_match_vv (haystack String, pattern String) ENGINE = Memory;
INSERT INTO test_match_vv VALUES ('', '.*'), ('', '.*?'), ('x', '.*'), ('x', '.*?');
SELECT haystack, pattern, match(haystack, pattern) FROM test_match_vv ORDER BY haystack, pattern;
DROP TABLE test_match_vv;
