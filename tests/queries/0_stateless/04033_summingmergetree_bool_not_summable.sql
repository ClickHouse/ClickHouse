-- Tags: no-fasttest
-- Verify that Bool columns are not summed by SummingMergeTree.
-- When SummingMergeTree() is used without explicit columns, it sums all
-- summable numeric columns. Bool (a domain over UInt8) should NOT be
-- considered summable, so it must be kept as-is (first value).
-- See https://github.com/ClickHouse/ClickHouse/issues/39965

DROP TABLE IF EXISTS test_summing_bool;

CREATE TABLE test_summing_bool
(
    `id` UInt32,
    `count` UInt64,
    `flag` Bool
)
ENGINE = SummingMergeTree
ORDER BY id;

-- Insert multiple rows with the same key
INSERT INTO test_summing_bool VALUES (1, 10, true), (1, 20, false), (1, 30, true);

OPTIMIZE TABLE test_summing_bool FINAL;

-- count should be summed (10+20+30=60)
-- flag should NOT be summed — it keeps the first inserted value (true)
-- Before the fix, flag was summed to 2 (true+false+true = 1+0+1)
SELECT id, count, flag FROM test_summing_bool;

-- Before the fix, this query returned 0, which is incorrect. After the fix, it should return 1.
SELECT count() FROM test_summing_bool where flag = true;

DROP TABLE test_summing_bool;
