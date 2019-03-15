DROP TABLE IF EXISTS test.add_materialized_column_after;

CREATE TABLE test.add_materialized_column_after (x UInt32, z UInt64) ENGINE MergeTree ORDER BY x;
ALTER TABLE test.add_materialized_column_after ADD COLUMN y MATERIALIZED toString(x) AFTER x;

DESC TABLE test.add_materialized_column_after;

DROP TABLE test.add_materialized_column_after;
