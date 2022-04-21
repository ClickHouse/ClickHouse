-- Tags: distributed

SET optimize_move_functions_out_of_any = 1;
SET optimize_injective_functions_inside_uniq = 1;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;
SET optimize_if_transform_strings_to_enum = 1;

SELECT any(number + 1) FROM numbers(1);
SELECT uniq(bitNot(number)) FROM numbers(1);
SELECT sum(number + 1) FROM numbers(1);
SELECT transform(number, [1, 2], ['google', 'yandex'], 'other') FROM numbers(1);
SELECT number > 0 ? 'yandex' : 'google' FROM numbers(1);


DROP TABLE IF EXISTS local_table;
DROP TABLE IF EXISTS dist;

CREATE TABLE local_table (number UInt64) ENGINE = Memory;
CREATE TABLE dist AS local_table ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_table);

INSERT INTO local_table SELECT number FROM numbers(1);

SELECT any(number + 1) FROM dist;
SELECT uniq(bitNot(number)) FROM dist;
SELECT sum(number + 1) FROM dist;
SELECT transform(number, [1, 2], ['google', 'yandex'], 'other') FROM dist;
SELECT number > 0 ? 'yandex' : 'google' FROM dist;

DROP TABLE local_table;
DROP TABLE dist;
