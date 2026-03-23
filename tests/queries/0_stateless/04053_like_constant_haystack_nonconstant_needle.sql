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
