-- Random settings limits: max_bytes_before_external_set=(0, 0); max_bytes_ratio_before_external_set=(0, 0)
-- The set of `IN` stays in memory: a set on disk throws `QUERY_WAS_CANCELLED` in the `break` mode.
DROP TABLE IF EXISTS t;
CREATE TABLE t (key UInt64, value UInt64, INDEX value_idx value TYPE bloom_filter GRANULARITY 1) ENGINE=MergeTree() ORDER BY key;

INSERT INTO t SELECT number, rand()%1000 FROM numbers(10000);

SET timeout_overflow_mode='break';
SET max_execution_time=0.1, max_rows_to_read=0;
SELECT * FROM t WHERE value IN (SELECT number FROM numbers(1000000000));

DROP TABLE t;
