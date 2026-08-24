CREATE TABLE test_02559__fuzz_20(`id1` Int16, `id2` Decimal(18, 14)) ENGINE = MergeTree ORDER BY id1;

INSERT INTO test_02559__fuzz_20 SELECT number, number FROM numbers(10);

SET enable_multiple_prewhere_read_steps=true, move_all_conditions_to_prewhere=true;

SELECT count() FROM test_02559__fuzz_20 PREWHERE (id2 >= 104) AND ((-9223372036854775808 OR (inf OR -2147483649 OR NULL) OR NULL) OR 1 OR ignore(ignore(id1) OR NULL, id1)) WHERE ignore(id1) = 0;
