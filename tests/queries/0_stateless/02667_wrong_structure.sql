DROP TABLE IF EXISTS test_table__fuzz_2;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE test_table__fuzz_2 (`id` LowCardinality(UInt64), `value` LowCardinality(String)) ENGINE = TinyLog;

INSERT INTO test_table__fuzz_2 VALUES (9806329011943062144,'wS6*');

SELECT arrayMap(x -> (id + (SELECT 1 AS id WHERE [10][NULL])), [1048575]) FROM test_table__fuzz_2;

DROP TABLE test_table__fuzz_2;
