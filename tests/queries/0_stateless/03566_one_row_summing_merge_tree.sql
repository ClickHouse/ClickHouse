DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A UInt32,
    B String
)
ENGINE = SummingMergeTree()
ORDER BY key;

INSERT INTO test_table Values(1,0,'');

SELECT count() FROM test_table;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A UInt32,
    B String
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_table Values(1,0,'');

SELECT count() FROM test_table;

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A Nullable(UInt32),
    B Nullable(String)
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

INSERT INTO test_table Values(1, 1,  'x');
SELECT * FROM test_table FINAL ORDER BY ALL;

INSERT INTO test_table Values(1, 2,  'y');
SELECT * FROM test_table FINAL ORDER BY ALL;

INSERT INTO test_table Values(1, 3,  'z');
SELECT * FROM test_table FINAL ORDER BY ALL;
