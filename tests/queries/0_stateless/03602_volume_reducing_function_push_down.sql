DROP TABLE IF EXISTS test_strings;

CREATE TABLE test_strings (
    id UInt32,
    s String,
    test_uint64 UInt64
) ENGINE = Memory;

INSERT INTO test_strings SELECT number, randomString(number % 10), number + 100000000 FROM system.numbers LIMIT 3;

SELECT length(s), length('constant'), test_uint64
FROM test_strings
WHERE s != ''
ORDER BY id
SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT length(s), length('constant'), test_uint64
FROM test_strings
WHERE s != ''
ORDER BY id
SETTINGS query_plan_push_down_volume_reducing_functions = 1;

EXPLAIN PIPELINE header=1 SELECT length(s), length('constant'), test_uint64
FROM test_strings
WHERE s != ''
ORDER BY id
SETTINGS query_plan_push_down_volume_reducing_functions = 0;

EXPLAIN PIPELINE header=1 SELECT length(s), length('constant'), test_uint64
FROM test_strings
WHERE s != ''
ORDER BY id
SETTINGS query_plan_push_down_volume_reducing_functions = 1;
