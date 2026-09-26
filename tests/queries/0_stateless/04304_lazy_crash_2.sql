-- https://github.com/ClickHouse/ClickHouse/issues/106095
-- Repro: LOGICAL_ERROR 'Bad cast from type DB::ColumnReplicated to DB::ColumnString'
-- Crash path: FinishSortingTransform::consume -> less (SortCursor.h) -> ColumnString::compareAt
-- Introduced by lazy column replication in ARRAY JOIN (PR #88752).
-- arrayJoin lazily replicates the `key` column into a ColumnReplicated; the read-in-order
-- sort comparison (ORDER BY prefix `key`) casts it straight to ColumnString and aborts.
-- Must run against a server (not clickhouse-local) and needs several parts so read-in-order
-- builds a FinishSortingTransform.

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    key String,
    val Map(String, Variant(String, Int32, DateTime64(3, 'UTC')))
)
ENGINE = MergeTree ORDER BY key
SETTINGS allow_experimental_variant_type = 1;

-- Keep one part per insert so read-in-order spreads ranges among streams.
SYSTEM STOP MERGES test;

INSERT INTO test VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10.11'});
INSERT INTO test VALUES ('', {'':'xx', '':4});
INSERT INTO test VALUES ('', {'x':'xx'});
INSERT INTO test VALUES ('', {});
INSERT INTO test VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10'});
INSERT INTO test VALUES ('a', {'a':'b', 'b':1, 'c': '2020-01-01'});
INSERT INTO test VALUES ('z', {'a':'a'});
INSERT INTO test VALUES ('a', {'a': Null});
INSERT INTO test VALUES ('a', {'a': Null, 'a': Null});
INSERT INTO test VALUES ('a', {'a': Null, 'c': Null});

-- Crashes the server:
SELECT key, arrayJoin(mapValues(val))
FROM test
ORDER BY ALL
SETTINGS
    allow_suspicious_types_in_order_by = 1,
    enable_lazy_columns_replication = 1,
    optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = false,
    read_in_order_two_level_merge_threshold = 0,
    max_threads = 1;

DROP TABLE IF EXISTS test;
