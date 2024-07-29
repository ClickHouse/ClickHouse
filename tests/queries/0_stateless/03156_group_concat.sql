DROP TABLE IF EXISTS test_groupConcat;
CREATE TABLE test_groupConcat
(
    id UInt64,
    p_int Int32 NULL,
    p_string String,
    p_array Array(Int32)
) ENGINE = MergeTree ORDER BY id;

SET max_insert_threads = 1, max_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO test_groupConcat VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, 'a', [993, 986, 979, 972]), (2, 123, 'makson95', []);

SELECT * FROM test_groupConcat;

SELECT groupConcat(p_int) FROM test_groupConcat;
SELECT groupConcat(p_string) FROM test_groupConcat;
SELECT groupConcat(p_array) FROM test_groupConcat;

SELECT groupConcat('', 1)(p_array) FROM test_groupConcat;
SELECT groupConcat('', 3)(p_string) FROM test_groupConcat;
SELECT groupConcat('', 2)(p_int) FROM test_groupConcat;
SELECT groupConcat('\n', 3)(p_int) FROM test_groupConcat;

SELECT groupConcat(',')(p_int) FROM test_groupConcat;
SELECT groupConcat(',')(p_string) FROM test_groupConcat;
SELECT groupConcat(',', 2)(p_array) FROM test_groupConcat;

SELECT groupConcat(p_int) FROM test_groupConcat WHERE id = 1;

INSERT INTO test_groupConcat VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, 'a', [993, 986, 979, 972]), (2, 123, 'makson95', []);
INSERT INTO test_groupConcat VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, 'a', [993, 986, 979, 972]), (2, 123, 'makson95', []);

SELECT groupConcat(p_int) FROM test_groupConcat;
SELECT groupConcat(',')(p_string) FROM test_groupConcat;
SELECT groupConcat(p_array) FROM test_groupConcat;

SELECT groupConcat(123)(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupConcat(',', '3')(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupConcat(',', 0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupConcat(',', -1)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupConcat(',', 3, 3)(number) FROM numbers(10); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

SELECT length(groupConcat(number)) FROM numbers(100000);

DROP TABLE IF EXISTS test_groupConcat;

CREATE TABLE test_groupConcat
(
    id UInt64,
    p_int Int32,
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_groupConcat SELECT number, number FROM numbers(100000) SETTINGS min_insert_block_size_rows = 2000;

SELECT length(groupConcat(p_int)) FROM test_groupConcat;

DROP TABLE IF EXISTS test_groupConcat;
