CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv (id UInt64) ENGINE = MergeTree ORDER BY id AS SELECT id FROM t;
INSERT INTO t SELECT number FROM numbers(1000);
SET max_table_size_to_drop = 1, max_partition_size_to_drop = 1;
DROP TABLE mv;  -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }
SET max_table_size_to_drop = 0, max_partition_size_to_drop = 0;
DROP TABLE mv;
