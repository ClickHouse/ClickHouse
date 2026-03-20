-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99920
-- Columns stored in HashJoin could share internal data with other columns
-- (e.g., subcolumns extracted from Dynamic type). When those parent columns
-- were later modified, the shared data would change, causing allocated_size
-- tracking to become inconsistent (exception in debug builds).

SET allow_experimental_correlated_subqueries = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS test_fuzz_99920;
CREATE TABLE test_fuzz_99920 (id Nullable(UInt64), d Dynamic(max_types = 133)) ENGINE = Memory;

INSERT INTO test_fuzz_99920 SELECT number, number FROM numbers(100000);
INSERT INTO test_fuzz_99920 SELECT number, concat('str_', toString(number)) FROM numbers(100000, 100000);
INSERT INTO test_fuzz_99920 SELECT number, arrayMap(x -> multiIf((number % 9) = 0, NULL, (number % 9) = 3, concat('str_', toString(number)), number), range((number % 10) + 1)) FROM numbers(200000, 100000);
INSERT INTO test_fuzz_99920 SELECT number, NULL FROM numbers(300000, 100000);

SELECT count() FROM test_fuzz_99920 WHERE NOT empty((SELECT d.`Array(Variant(String, UInt64))`));

DROP TABLE test_fuzz_99920;
