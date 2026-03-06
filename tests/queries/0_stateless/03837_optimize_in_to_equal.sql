DROP TABLE IF EXISTS test_in_to_equal;
CREATE TABLE test_in_to_equal (x String, y Int32, z Nullable(Int32)) ENGINE = MergeTree() ORDER BY x;
INSERT INTO test_in_to_equal VALUES ('a', 1, 1), ('b', 2, 2), ('c', 3, NULL);

SET optimize_in_to_equal = 1;

-- Basic: x IN ('a') → x = 'a'
SELECT * FROM test_in_to_equal WHERE x IN ('a');

SELECT '---';

-- Basic: x NOT IN ('a') → x != 'a'
SELECT * FROM test_in_to_equal WHERE x NOT IN ('a');

SELECT '---';

-- Multiple values: should NOT be converted
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x IN ('a', 'b');

SELECT '---';

-- Array: should NOT be converted
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x IN ['a', 'b'];

SELECT '---';

-- NULL value: should NOT be converted (x IN NULL ≠ x = NULL semantically)
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x IN (NULL);

SELECT '---';
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x NOT IN (NULL);

SELECT '---';

-- Verify conversion happens: query tree should show equals
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x IN ('a');

SELECT '---';

-- Verify conversion happens: query tree should show notEquals
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x NOT IN ('a');

SELECT '---';

-- Expression in IN: x IN (upper('a')) should still convert (constant after folding)
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x IN (upper('a'));

SELECT '---';

-- Nullable column: should still convert
SELECT * FROM test_in_to_equal WHERE z IN (1);

SELECT '---';

-- Fuzzed case from issue #62129: arrayExists with IN and toNullable
SELECT number FROM numbers(2) WHERE arrayExists(_ -> (_ IN toNullable(4294967290)), [number]);

SELECT '---';

-- Verify setting can disable the optimization
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE x IN ('a')
SETTINGS optimize_in_to_equal = 0;

SELECT '---';

-- Integer type: y IN (1)
SELECT * FROM test_in_to_equal WHERE y IN (1);

SELECT '---';

-- Integer type: y NOT IN (1)
SELECT * FROM test_in_to_equal WHERE y NOT IN (1);

SELECT '---';

-- Type incompatibility: Date IN (scalar) should NOT be converted
-- (equals rejects Date vs Number, but IN accepts it)
SELECT toDate('2024-01-01') IN (1);

DROP TABLE test_in_to_equal;
