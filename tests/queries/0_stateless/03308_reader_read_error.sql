DROP TABLE IF EXISTS account_test__fuzz_3;
CREATE TABLE account_test__fuzz_3
(
    id Int16,
    row_ver UInt64
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity_bytes = 0, index_granularity = 42;

INSERT INTO account_test__fuzz_3 FORMAT Values (0, 0);

SELECT * FROM account_test__fuzz_3 PREWHERE 4;
