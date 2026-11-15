DROP TABLE IF EXISTS test;
CREATE TABLE test
    (
        x UInt8,
        INDEX x_idx x TYPE minmax GRANULARITY 1,
        z String DEFAULT 'Hello'
    )
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns=0, min_bytes_for_wide_part=1000, alter_column_secondary_index_mode='throw';
ALTER TABLE test CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY SETTING alter_column_secondary_index_mode='compatibility';
ALTER TABLE test CLEAR COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY SETTING alter_column_secondary_index_mode='rebuild';
INSERT INTO test (x) SELECT number FROM numbers(1); -- Compact / packed
INSERT INTO test (x) SELECT number FROM numbers(10000); -- Wide
SELECT 'alias_before_clear', min(x), max(x) FROM test;
ALTER TABLE test DROP COLUMN x; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE test CLEAR COLUMN x;
SELECT 'alias_after_clear', min(x), max(x) FROM test;
DROP TABLE test;