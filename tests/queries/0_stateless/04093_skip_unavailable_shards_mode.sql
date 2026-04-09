-- Tags: shard

-- Test skip_unavailable_shards_mode setting with different modes.

-- Mode 'unavailable' (default): only connection failures are skipped, not query exceptions.
SELECT * FROM remote('127.{1,2}', view(SELECT throwIf(1, 'simulated error') AS x))
    SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Mode 'unavailable_or_table_missing': skip UNKNOWN_TABLE / UNKNOWN_DATABASE / TABLE_IS_DROPPED.
SELECT 'table_missing:';
SELECT * FROM remote('127.{1,2}', currentDatabase(), 'nonexistent_table_04093')
    SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing';

-- But arbitrary exceptions are still raised in 'unavailable_or_table_missing' mode.
SELECT * FROM remote('127.{1,2}', view(SELECT throwIf(1, 'simulated error') AS x))
    SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Mode 'unavailable_or_exception_before_processing': any exception before data processing is skipped.
SELECT 'exception_before_processing:';
SELECT * FROM remote('127.{1,2}', view(SELECT throwIf(1, 'simulated error') AS x))
    SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';
