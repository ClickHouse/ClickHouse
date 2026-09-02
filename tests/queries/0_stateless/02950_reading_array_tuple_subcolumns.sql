DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    `id` UInt64,
    `t` Tuple(a UInt64, b Array(Tuple(c UInt64, d UInt64)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;
INSERT INTO test SELECT number, tuple(number, arrayMap(x -> tuple(number + 1, number + 2), range(number % 10))) FROM numbers(100000);
INSERT INTO test SELECT number, tuple(number, arrayMap(x -> tuple(number + 1, number + 2), range(number % 10))) FROM numbers(100000);
INSERT INTO test SELECT number, tuple(number, arrayMap(x -> tuple(number + 1, number + 2), range(number % 10))) FROM numbers(100000);
SELECT t.b, t.b.c FROM test ORDER BY id FORMAT Null;
DROP TABLE test;

