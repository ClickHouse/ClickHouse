DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt32,
    A Nullable(UInt32),
    B Nullable(String),
    C Tuple(Nullable(String))
)
ENGINE = CoalescingMergeTree()
ORDER BY key;

SET optimize_on_insert = 0;

INSERT INTO test_table SELECT 1, Null, '', tuple(toNullable('xxx')) x;

OPTIMIZE TABLE test_table FINAL;

SELECT * FROM test_table FINAL;

DROP TABLE test_table;
