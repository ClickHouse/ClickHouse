DROP TABLE IF EXISTS dist_with_interpreter_for_sharding_key;
CREATE TABLE dist_with_interpreter_for_sharding_key (key Int) ENGINE=Distributed(test_shard_localhost, system, one, 1 IN (SELECT 2));

SELECT * FROM remote('127.0.0.{3|2}', numbers(2), number NOT IN (SELECT number FROM numbers(1))) FORMAT Null;

-- found by fuzzer
SELECT number FROM remote('127.0.0.{3|2}', numbers(2), number NOT IN (SELECT number FROM numbers(1) FINAL WHERE 1025 QUALIFY equals(number) = number)) WHERE (materialize(number IN (SELECT number FROM numbers(1) WHERE toNullable(1) = number GROUP BY 1, number = (toInt256(10) = number)), 10) = number) OR (number NOT IN (SELECT DISTINCT number FROM numbers(1) WHERE -1 QUALIFY (1 = number) = number)) OR (number = 1) OR (toInt128(toInt128(3)) <= number) WITH TOTALS HAVING 1; -- { serverError ILLEGAL_FINAL }
