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

-- Nullable column: should NOT be converted (IN and equals have different NULL semantics)
-- IN returns 0/1 (UInt8) for NULL inputs, equals returns NULL (Nullable(UInt8))
SELECT * FROM test_in_to_equal WHERE z IN (1);

SELECT '---';

-- Verify Nullable column keeps IN in query tree
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE z IN (1);

SELECT '---';

-- Verify Nullable NOT IN also keeps notIn
EXPLAIN QUERY TREE SELECT * FROM test_in_to_equal WHERE z NOT IN (1);

SELECT '---';

-- Nullable semantics: NOT IN returns 1 for NULL, != returns NULL
-- This verifies the optimization is correctly skipped
SELECT z NOT IN (1) FROM test_in_to_equal ORDER BY x;

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

SELECT '---';

-- Enum: should NOT be converted (equals/notEquals throw for unknown enum values,
-- but IN/NOT IN silently treat them as non-matching)
DROP TABLE IF EXISTS test_enum_in;
CREATE TABLE test_enum_in (e Enum('a' = 1, 'b' = 2)) ENGINE = Memory;
INSERT INTO test_enum_in VALUES ('a');

-- Valid enum value: IN works, equals would also work, but we skip Enum entirely for safety
EXPLAIN QUERY TREE SELECT * FROM test_enum_in WHERE e IN ('a');

SELECT '---';

-- Unknown enum value: IN returns empty, equals would throw UNKNOWN_ELEMENT_OF_ENUM
SELECT * FROM test_enum_in WHERE e IN ('c');

SELECT '---';

-- Unknown enum value with NOT IN: returns all rows; notEquals would throw
SELECT * FROM test_enum_in WHERE e NOT IN ('c');

DROP TABLE test_enum_in;

SELECT '---';

-- LowCardinality(Nullable): should NOT be converted (same NULL semantics issue)
DROP TABLE IF EXISTS test_lc_nullable;
CREATE TABLE test_lc_nullable (s LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO test_lc_nullable VALUES ('a'), ('b'), (NULL);

EXPLAIN QUERY TREE SELECT * FROM test_lc_nullable WHERE s IN ('a');

SELECT '---';

SELECT s NOT IN ('a') FROM test_lc_nullable ORDER BY s NULLS LAST;

DROP TABLE test_lc_nullable;

DROP TABLE test_in_to_equal;
