SET enable_analyzer = 1;

CREATE TABLE test__fuzz_1 (`id` Nullable(UInt64), `d` Dynamic(max_types = 133)) ENGINE = Memory;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, number FROM numbers(100000) SETTINGS min_insert_block_size_rows = 50000;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, concat('str_', toString(number)) FROM numbers(100000, 100000) SETTINGS min_insert_block_size_rows = 50000;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, arrayMap(x -> multiIf((number % 9) = 0, NULL, (number % 9) = 3, concat('str_', toString(number)), number), range((number % 10) + 1)) FROM numbers(200000, 100000) SETTINGS min_insert_block_size_rows = 50000;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, NULL FROM numbers(300000, 100000) SETTINGS min_insert_block_size_rows = 50000;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, multiIf((number % 4) = 3, concat('str_', toString(number)), (number % 4) = 2, NULL, (number % 4) = 1, number, arrayMap(x -> multiIf((number % 9) = 0, NULL, (number % 9) = 3, concat('str_', toString(number)), number), range((number % 10) + 1))) FROM numbers(400000, 400000) SETTINGS min_insert_block_size_rows = 50000;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, if((number % 5) = 1, CAST([range(CAST((number % 10) + 1, 'UInt64'))], 'Array(Array(Dynamic))'), number) FROM numbers(100000, 100000) SETTINGS min_insert_block_size_rows = 50000;

INSERT INTO test__fuzz_1 SETTINGS min_insert_block_size_rows = 50000 SELECT number, if((number % 5) = 1, CAST(CAST(concat('str_', number), 'LowCardinality(String)'), 'Dynamic'), CAST(number, 'Dynamic')) FROM numbers(100000, 100000) SETTINGS min_insert_block_size_rows = 50000;

SELECT count(equals(toInt256(257), intDiv(65536, -2147483649) AS alias144)), toInt128(2147483647) FROM test__fuzz_1 WHERE NOT empty((SELECT d.`Array(Variant(String, UInt64))`));

DROP TABLE test__fuzz_1;

