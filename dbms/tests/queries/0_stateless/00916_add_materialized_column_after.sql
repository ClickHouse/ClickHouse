DROP TABLE IF EXISTS add_materialized_column_after;

CREATE TABLE add_materialized_column_after (x UInt32, z UInt64) ENGINE MergeTree ORDER BY x;
ALTER TABLE add_materialized_column_after ADD COLUMN y MATERIALIZED toString(x) AFTER x;

DESC TABLE add_materialized_column_after;

DROP TABLE add_materialized_column_after;
