-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: Parquet is not available in fasttest.
-- no-random-settings, no-random-merge-tree-settings: the selected marks and EXPLAIN output may differ with
-- random settings.

SET explain_query_plan_default = 'legacy';

CREATE TABLE test_consumers (ts DateTime('UTC'), v UInt32) ENGINE = MergeTree ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_consumers SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + INTERVAL intDiv(number, 4) DAY + INTERVAL (number % 4) * 6 HOUR, number FROM numbers(24);

-- Query condition cache: the second run must return the same result
-- (a wrong pruning verdict would poison the cache).
SELECT count() FROM test_consumers WHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC') SETTINGS use_query_condition_cache = 1;
SELECT count() FROM test_consumers WHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC') SETTINGS use_query_condition_cache = 1;
SELECT count() FROM test_consumers WHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC') SETTINGS use_query_condition_cache = 0, use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- PREWHERE over the multi-expression key. The line prints the predicate, the count with index
-- analysis, the count of a full scan, the marks selected by index analysis and the key condition.
SELECT 'PREWHERE ts > toDateTime(\'2026-04-01 00:00:00\', \'UTC\')', (SELECT count() FROM test_consumers PREWHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC')),
    (SELECT count() FROM test_consumers PREWHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0),
    (SELECT sum(marks) FROM (EXPLAIN ESTIMATE SELECT 1 FROM test_consumers PREWHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC'))),
    (SELECT arrayStringConcat(groupArray(trimLeft(explain)), ' | ') FROM (EXPLAIN indexes = 1 SELECT 1 FROM test_consumers PREWHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC')) WHERE trimLeft(explain) LIKE 'Condition:%')
FORMAT TSVRaw;

-- Reading in order with a multi-atom predicate.
SELECT ts FROM test_consumers WHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC') ORDER BY ts LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT ts FROM test_consumers WHERE ts > toDateTime('2026-04-01 00:00:00', 'UTC') ORDER BY ts LIMIT 3 SETTINGS optimize_read_in_order = 0, use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Parquet pushdown smoke: the WHERE clause becomes a KeyCondition over the columns of the
-- file, used for row-group min-max pruning, bloom-filter pruning, and per-column
-- page pruning (via extractSingleColumnConditions). Small row groups make the
-- pruning actually engage; wrong row-group/page skipping would change the results.
INSERT INTO FUNCTION file('04336_data_' || currentDatabase() || '.parquet', Parquet, 'c0 Int32, c1 Int32') SELECT number, number * 10 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 10;
SELECT count() FROM file('04336_data_' || currentDatabase() || '.parquet', Parquet, 'c0 Int32, c1 Int32') WHERE c0 > 90 AND c1 < 980 SETTINGS log_comment = '04336 parquet pruning range';
SELECT sum(c1) FROM file('04336_data_' || currentDatabase() || '.parquet', Parquet, 'c0 Int32, c1 Int32') WHERE c0 IN (5, 50, 95) SETTINGS log_comment = '04336 parquet pruning in';

-- The amount of row-group pruning is observable via ProfileEvents.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParquetPrunedRowGroups'], ProfileEvents['ParquetReadRowGroups'] FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '04336 parquet pruning range' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['ParquetPrunedRowGroups'], ProfileEvents['ParquetReadRowGroups'] FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '04336 parquet pruning in' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1;
