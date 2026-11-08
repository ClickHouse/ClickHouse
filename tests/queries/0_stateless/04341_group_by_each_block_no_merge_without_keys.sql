-- `group_by_each_block_no_merge` is a streaming GROUP BY mode. Aggregation without grouping keys has a single
-- group, so the setting must not change its semantics: such a query is always fully merged and returns exactly
-- one row, including the single empty-aggregation row for an empty input.

-- Empty input: the single empty-aggregation row is still produced (not zero rows).
SELECT count() FROM numbers(0) SETTINGS group_by_each_block_no_merge = 1;

-- Keyless aggregation over empty input returns exactly one row.
SELECT count() FROM (SELECT count(), sum(number) FROM numbers(0) SETTINGS group_by_each_block_no_merge = 1);

-- Keyless aggregation is fully merged into one row, not flushed per block, even with many small blocks.
SELECT count() FROM (SELECT count(), sum(number) FROM numbers(10000) SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000);

-- The result matches plain aggregation: the setting has no effect without grouping keys.
SELECT
    (SELECT (count(), sum(number), min(number), max(number)) FROM numbers(12345) SETTINGS group_by_each_block_no_merge = 1, max_block_size = 100)
  = (SELECT (count(), sum(number), min(number), max(number)) FROM numbers(12345));
