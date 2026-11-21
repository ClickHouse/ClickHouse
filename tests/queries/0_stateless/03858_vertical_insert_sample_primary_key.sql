-- Test: PRIMARY KEY and SAMPLE BY paths work with vertical insert when using a materialized key expression.
DROP TABLE IF EXISTS t_vi_sample_pk;

CREATE TABLE t_vi_sample_pk
(
    k UInt64,
    d Date,
    s String,
    k_hash UInt64 MATERIALIZED cityHash64(k)
)
ENGINE = MergeTree
ORDER BY (d, k_hash)
PRIMARY KEY (d, k_hash)
SAMPLE BY k_hash
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_sample_pk
SELECT
    number,
    toDate('2020-01-01'),
    toString(number)
FROM numbers(10);

SELECT count() FROM t_vi_sample_pk;

SELECT
    primary_key = 'd, k_hash'
    AND sorting_key = 'd, k_hash'
    AND sampling_key = 'k_hash'
FROM system.tables
WHERE database = currentDatabase()
  AND name = 't_vi_sample_pk';

DROP TABLE t_vi_sample_pk;
