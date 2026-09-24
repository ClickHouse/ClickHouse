-- Tags: no-random-settings, no-random-merge-tree-settings
-- The selected mark counts depend on fixed granularity and index-analysis settings.

SET use_statistics_for_part_pruning = 0;
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_move_to_prewhere = 0;

-- Wrapped constant modes retain their numbering convention. Monday-first numbering prunes
-- to three marks for Saturday and Sunday; Sunday-first numbering must retain the full week.
DROP TABLE IF EXISTS test_day_of_week;
CREATE TABLE test_day_of_week (d Date32) ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_day_of_week SELECT toDate32('2026-08-03') + number FROM numbers(7);

SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(1))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(1))) >= 5
SETTINGS max_rows_to_read = 3;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(6))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(6))) >= 5
SETTINGS max_rows_to_read = 7;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(1))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(1))) >= 5
SETTINGS max_rows_to_read = 3;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(6))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(6))) >= 5
SETTINGS max_rows_to_read = 7;
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, CAST(NULL AS Nullable(UInt8))) >= 5;
DROP TABLE test_day_of_week;

DROP TABLE IF EXISTS test_day_of_week;
CREATE TABLE test_day_of_week (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_day_of_week SELECT toDateTime64('2026-08-03 12:00:00', 3, 'UTC') + number * 86400 FROM numbers(7);

SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(1))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(1))) >= 5
SETTINGS max_rows_to_read = 3;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(6))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toNullable(toUInt8(6))) >= 5
SETTINGS max_rows_to_read = 7;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(1))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(1))) >= 5
SETTINGS max_rows_to_read = 3;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(6))) >= 5);
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, toLowCardinality(toUInt8(6))) >= 5
SETTINGS max_rows_to_read = 7;
SELECT count() FROM test_day_of_week WHERE toDayOfWeek(d, CAST(NULL AS Nullable(UInt8))) >= 5;
DROP TABLE test_day_of_week;

-- Default wrappers preserve execution for dynamic values and for null-only arguments whose
-- handling bypasses validation of the other argument types.
SELECT arrayMap(d -> toDayOfWeek(d, 1), CAST([toDate('2026-08-03'), toDate('2026-08-09')] AS Array(Dynamic)));
SELECT arrayMap(d -> toDayOfWeek(d, 6), CAST([toDate32('2026-08-03'), toDate32('2026-08-09')] AS Array(Variant(Date32, String))));
SELECT toDayOfWeek(NULL, 'unused'), toDayOfWeek(toDate('2026-08-03'), NULL);
SELECT arrayMap(d -> toDayOfWeek(d, 'unused'), CAST([] AS Array(Nothing)));
