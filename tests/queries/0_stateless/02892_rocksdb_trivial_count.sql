-- Tags: use-rocksdb

CREATE TABLE dict (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO dict SELECT number, toString(number) FROM numbers(121);
-- { echoOn }
SELECT count() FROM dict SETTINGS optimize_trivial_approximate_count_query = 0, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM dict SETTINGS optimize_trivial_approximate_count_query = 1, max_rows_to_read = 1;
SET optimize_trivial_approximate_count_query = 1;
-- needs more data to see total_bytes or just detach and attach the table
DETACH TABLE dict SYNC;
ATTACH TABLE dict;
SELECT total_rows, total_bytes > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'dict' FORMAT CSV;
