CREATE TABLE t_multiply_i128_overflow (key Int128) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1, auto_statistics_types = 'minmax, uniq';
INSERT INTO t_multiply_i128_overflow SELECT number FROM numbers(10);
INSERT INTO t_multiply_i128_overflow VALUES (-56713727820156410577229101238628035243);
OPTIMIZE TABLE t_multiply_i128_overflow FINAL;
SELECT count(*) FROM t_multiply_i128_overflow WHERE key <= 10 and key >= 0;

DROP TABLE t_multiply_i128_overflow;
