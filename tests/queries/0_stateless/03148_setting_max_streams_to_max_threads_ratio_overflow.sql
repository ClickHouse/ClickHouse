DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value_0');

SELECT * FROM test_table SETTINGS max_threads = 1025, max_streams_to_max_threads_ratio = -9223372036854775808, enable_analyzer = 1; -- { serverError PARAMETER_OUT_OF_BOUND }

SELECT * FROM test_table SETTINGS max_threads = 1025, max_streams_to_max_threads_ratio = -9223372036854775808, enable_analyzer = 0; -- { serverError PARAMETER_OUT_OF_BOUND }

DROP TABLE test_table;
