-- TODO: Make sure we test with wide, compact and packed parts
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,

    value String,
    INDEX idx_ip_set (value) TYPE set(0) GRANULARITY 1,

    -- We will test ALTER COLUMN that would modify the index to an incompatible type
    ip String,
    INDEX idx_ip_bloom (ip) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1,
)
ENGINE = MergeTree()
ORDER BY id
PARTITION BY id
SETTINGS alter_column_secondary_index_mode = 'rebuild';

-- Need it for proper explain
SET use_skip_indexes_on_data_read = 0;
-- Need it because we rerun some queries (with different settings) and we want to execute the full analysis
SET use_query_condition_cache = 0;

INSERT INTO test_table VALUES (1, 'a', '127.0.0.1'), (2, 'b', '127.0.0.2'), (3, 'c', '127.0.0.3');

SELECT 'IP column tests';
EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE ip = '127.0.0.1';
SELECT count() FROM test_table WHERE ip = '127.0.0.1';
-- If we try to modify the column to a type incompatible with the index it should always throw an error and forbid the ALTER,
-- independently of the alter_column_secondary_index_mode setting.
ALTER TABLE test_table MODIFY COLUMN ip IPv4; -- { serverError INCORRECT_QUERY }

SELECT 'STRING TO FIXEDSTRING tests';
EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = 'c';
SELECT count() FROM test_table WHERE value = 'c';

SYSTEM STOP MERGES test_table;
SET alter_sync = 0;
ALTER TABLE test_table MODIFY COLUMN value FixedString(1);
-- At this point the index is incompatible with the old parts so it should be ignored
SELECT 'Status after ALTER is issued, before merges happen, max_threads=1';
SET max_threads = 1;
EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = 'c';
SELECT count() FROM test_table WHERE value = 'c';

SELECT 'Status after ALTER is issued, before merges happen, max_threads=2;';
SET max_threads = 2;
EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = 'c';
SELECT count() FROM test_table WHERE value = 'c';

SET max_threads = DEFAULT;
SYSTEM START MERGES test_table;
-- Wait for mutations
SET alter_sync = 1;
ALTER TABLE test_table DELETE WHERE value = 'DoesNotExist'; -- Just a dummy mutation to wait for previous to finish

SELECT 'Status after ALTER is applied';
EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = 'c';
SELECT count() FROM test_table WHERE value = 'c';

-- Force a table rewrite to confirm it's the same
OPTIMIZE TABLE test_table FINAL;
SELECT 'Status after ALTER full table rewrite (should be the same)';
EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = 'c';
SELECT count() FROM test_table WHERE value = 'c';
