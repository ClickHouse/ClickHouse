-- Test for LikeToRangeRewritePass optimization in analyzer
-- This pass should rewrite LIKE 'prefix%' expressions to range conditions for better performance

SET optimize_rewrite_like_to_range = 1;

DROP TABLE IF EXISTS test_like_rewrite;

CREATE TABLE test_like_rewrite (
    id UInt32,
    name String,
    category FixedString(10)
) ENGINE = MergeTree()
ORDER BY name;

INSERT INTO test_like_rewrite VALUES 
    (1, 'apple', 'fruit'),
    (2, 'application', 'software'),
    (3, 'apply', 'verb'),
    (4, 'banana', 'fruit'),
    (5, 'band', 'music'),
    (6, 'test', 'other'),
    (7, 'testing', 'other');

-- Test perfect prefix patterns - should be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE 'app%';
SELECT count() FROM test_like_rewrite WHERE name LIKE 'app%';

EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE 'test%';
SELECT count() FROM test_like_rewrite WHERE name LIKE 'test%';

-- Test with FixedString column
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE category LIKE 'fruit%';
SELECT count() FROM test_like_rewrite WHERE category LIKE 'fruit%';

-- Test NOT LIKE with perfect prefix - should be rewritten (TODO)
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name NOT LIKE 'app%';
SELECT count() FROM test_like_rewrite WHERE name NOT LIKE 'app%';

-- Test imperfect prefix patterns - should NOT be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE 'app_ication%';
SELECT count() FROM test_like_rewrite WHERE name LIKE 'app_ication%';

-- Test patterns that should NOT be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE '%app%';
SELECT count() FROM test_like_rewrite WHERE name LIKE '%app%';

EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE '_est%';
SELECT count() FROM test_like_rewrite WHERE name LIKE '_est%';

-- Test exact match (no wildcards) - should NOT be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE 'test';
SELECT count() FROM test_like_rewrite WHERE name LIKE 'test';

-- Test empty prefix - should NOT be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE '%';
SELECT count() FROM test_like_rewrite WHERE name LIKE '%';

-- Test case-insensitive ILIKE with perfect prefix - should be rewritten (TODO)
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name ILIKE 'APP%';
SELECT count() FROM test_like_rewrite WHERE name ILIKE 'APP%';

-- Test multiple LIKE conditions - all eligible ones should be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE 'app%' AND category LIKE 'fruit%';
SELECT count() FROM test_like_rewrite WHERE name LIKE 'app%' AND category LIKE 'fruit%';

-- Test LIKE in OR condition
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name LIKE 'app%' OR name LIKE 'test%';
SELECT count() FROM test_like_rewrite WHERE name LIKE 'app%' OR name LIKE 'test%';

-- Test NOT LIKE with imperfect prefix - should NOT be rewritten
EXPLAIN SYNTAX SELECT * FROM test_like_rewrite WHERE name NOT LIKE 'app_ication%';
SELECT count() FROM test_like_rewrite WHERE name NOT LIKE 'app_ication%';

DROP TABLE test_like_rewrite;
