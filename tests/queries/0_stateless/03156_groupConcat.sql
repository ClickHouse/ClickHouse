DROP TABLE IF EXISTS test_groupConcat;
CREATE TABLE IF NOT EXISTS test_groupConcat
(
    id UInt64,
    p_int Int32 NULL,
    p_string String,
    p_array Array(Int32)
) ENGINE = MergeTree
  PRIMARY KEY id;

INSERT INTO test_groupConcat VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, '', [993, 986, 979, 972]), (2, 123, 'makson95', []);

SELECT groupConcat(p_int) FROM test_groupConcat;
SELECT groupConcat(p_string) FROM test_groupConcat;
SELECT groupConcat(p_array) FROM test_groupConcat;

SELECT groupConcat(',')(p_int) FROM test_groupConcat;
SELECT groupConcat(',')(p_string) FROM test_groupConcat;
SELECT groupConcat(',', 2)(p_array) FROM test_groupConcat;

SELECT groupConcat(p_int) FROM test_groupConcat WHERE id = 1;

SELECT groupConcat(123)(number) FROM system.numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupConcat(',', '3')(number) FROM system.numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupConcat(',', 0)(number) FROM system.numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupConcat(',', -1)(number) FROM system.numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupConcat(',', 3, 3)(number) FROM system.numbers(10); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
