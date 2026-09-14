-- Verify that automatic LowCardinality serialization materializes constant columns before building a dictionary.
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

CREATE TABLE t_auto_lc_const_insert
(
    id UInt64,
    value String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 10;

INSERT INTO t_auto_lc_const_insert SELECT number, 'constant' FROM numbers(100);

SELECT count(), any(value), uniqExact(value) FROM t_auto_lc_const_insert;
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_const_insert' AND active AND column = 'value';

DROP TABLE t_auto_lc_const_insert;
