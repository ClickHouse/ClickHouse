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

SELECT * FROM test_table;

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

SELECT * FROM test_table;
