-- Test for issue #86540: Out of bounds access with duplicate partition keys
-- This test verifies that duplicate partition field names don't cause crashes

DROP TABLE IF EXISTS test_duplicate_partition_keys;

-- Test case 1: Duplicate column names in partition expression
CREATE TABLE test_duplicate_partition_keys (
    c0 String,
    c1 Int32
)
ENGINE = MergeTree()
PARTITION BY (c1, c1, sipHash64(c0))
ORDER BY c0;

-- Insert some test data
INSERT INTO test_duplicate_partition_keys VALUES ('test1', 1), ('test2', 2), ('test3', 1);

-- This SELECT should not crash (was causing segfault before fix)
SELECT count() FROM test_duplicate_partition_keys WHERE c1 = 1;

-- Test the SELECT with different conditions
SELECT c0, c1 FROM test_duplicate_partition_keys WHERE c1 = 2 ORDER BY c0;

DROP TABLE test_duplicate_partition_keys;

-- Test case 2: More complex duplicate partition expression
CREATE TABLE test_duplicate_partition_keys2 (
    a String,
    b Int32,
    c Int32
)
ENGINE = MergeTree()
PARTITION BY (b, c, b, sipHash64(a))
ORDER BY a;

INSERT INTO test_duplicate_partition_keys2 VALUES ('x', 10, 20), ('y', 10, 30);

-- This should also work without crashing
SELECT count() FROM test_duplicate_partition_keys2 WHERE b = 10;

DROP TABLE test_duplicate_partition_keys2;

-- Test case 3: Simple table with duplicate keys in different positions
CREATE TABLE test_triple_duplicate (
    x UInt32,
    y String
)
ENGINE = MergeTree()
PARTITION BY (x, x, x)
ORDER BY y;

INSERT INTO test_triple_duplicate VALUES (1, 'a'), (2, 'b'), (1, 'c');

-- Test SELECT with triple duplicate partition keys
SELECT count() FROM test_triple_duplicate WHERE x = 1;

DROP TABLE test_triple_duplicate;

-- Test case 4: Mixed expression duplicates with date functions
CREATE TABLE test_mixed_duplicates (
    id Int32,
    name String,
    create_date Date
)
ENGINE = MergeTree()
PARTITION BY (id, toYYYYMM(create_date), id, sipHash64(name))
ORDER BY name;

INSERT INTO test_mixed_duplicates VALUES (1, 'test1', '2024-01-01'), (2, 'test2', '2024-02-01'), (1, 'test3', '2024-01-01');
SELECT count() FROM test_mixed_duplicates WHERE id = 1;

DROP TABLE test_mixed_duplicates;

-- Test case 5: Different data types with duplicates
CREATE TABLE test_type_duplicates (
    uint_col UInt32,
    int_col Int64,
    str_col String
)
ENGINE = MergeTree()
PARTITION BY (uint_col, int_col, uint_col, str_col, uint_col)
ORDER BY str_col;

INSERT INTO test_type_duplicates VALUES (100, -200, 'abc'), (200, -400, 'def'), (100, -200, 'xyz');
SELECT count() FROM test_type_duplicates WHERE uint_col = 100;

DROP TABLE test_type_duplicates;

-- Test case 6: Complex hash function duplicates
CREATE TABLE test_hash_duplicates (
    id UInt64,
    data String
)
ENGINE = MergeTree()
PARTITION BY (sipHash64(data), cityHash64(data), sipHash64(data))
ORDER BY id;

INSERT INTO test_hash_duplicates VALUES (1, 'sample1'), (2, 'sample2'), (3, 'sample1');
SELECT count() FROM test_hash_duplicates WHERE data = 'sample1';

DROP TABLE test_hash_duplicates;

-- Test case 7: Extreme case - single field repeated many times
CREATE TABLE test_extreme_repeats (
    x UInt64
)
ENGINE = MergeTree()
PARTITION BY (x, x, x, x, x, x)
ORDER BY x;

INSERT INTO test_extreme_repeats VALUES (1), (2), (1), (3);
SELECT count() FROM test_extreme_repeats WHERE x = 1;

DROP TABLE test_extreme_repeats;

-- Test case 8: Performance test with moderate data
CREATE TABLE test_performance_duplicates (
    category UInt32,
    name String
)
ENGINE = MergeTree()
PARTITION BY (category, category, category)
ORDER BY name;

INSERT INTO test_performance_duplicates SELECT number % 10, concat('name', toString(number)) FROM numbers(1000);
SELECT count() FROM test_performance_duplicates WHERE category = 5;

DROP TABLE test_performance_duplicates;