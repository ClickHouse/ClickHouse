-- `ATTACH PARTITION FROM` and `REPLACE PARTITION FROM` that would exceed the 'max_table_size_bytes_*' limits are rejected as a whole.

DROP TABLE IF EXISTS t_max_size_bytes_src;
DROP TABLE IF EXISTS t_max_size_bytes_dst;

CREATE TABLE t_max_size_bytes_src (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO t_max_size_bytes_src SELECT 1, number FROM numbers(100000);

CREATE TABLE t_max_size_bytes_dst (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x
    SETTINGS max_table_size_bytes_compressed = 1024;

ALTER TABLE t_max_size_bytes_dst ATTACH PARTITION 1 FROM t_max_size_bytes_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
ALTER TABLE t_max_size_bytes_dst REPLACE PARTITION 1 FROM t_max_size_bytes_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_bytes_dst;

ALTER TABLE t_max_size_bytes_dst MODIFY SETTING max_table_size_bytes_compressed = 0, max_table_size_bytes_uncompressed = 1024;

ALTER TABLE t_max_size_bytes_dst ATTACH PARTITION 1 FROM t_max_size_bytes_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
ALTER TABLE t_max_size_bytes_dst REPLACE PARTITION 1 FROM t_max_size_bytes_src; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM t_max_size_bytes_dst;

ALTER TABLE t_max_size_bytes_dst MODIFY SETTING max_table_size_bytes_uncompressed = 0;
ALTER TABLE t_max_size_bytes_dst ATTACH PARTITION 1 FROM t_max_size_bytes_src;
SELECT count() FROM t_max_size_bytes_dst;

DROP TABLE t_max_size_bytes_dst;
DROP TABLE t_max_size_bytes_src;
