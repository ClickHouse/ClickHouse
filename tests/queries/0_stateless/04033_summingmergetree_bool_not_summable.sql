-- Tags: no-fasttest
-- Verify that Bool columns are not summed by SummingMergeTree.
-- Bool should not be aggregated (kept as first value), not arithmetically summed.
-- See https://github.com/ClickHouse/ClickHouse/issues/39965

DROP TABLE IF EXISTS test_summing_bool;

CREATE TABLE test_summing_bool
(
    `id` UInt32,
    `count` UInt64,
    `flag` Bool
)
ENGINE = SummingMergeTree(count)
ORDER BY id;

-- Insert multiple rows with the same key
INSERT INTO test_summing_bool VALUES (1, 10, true), (1, 20, false), (1, 30, true);

OPTIMIZE TABLE test_summing_bool FINAL;

-- 'count' should be summed (60), 'flag' should NOT be summed
-- The underlying UInt8 value of flag must remain a valid Bool (0 or 1), not 2 or 3
SELECT id, count, toUInt8(flag) <= 1 as flag_is_valid_bool FROM test_summing_bool;

DROP TABLE test_summing_bool;
