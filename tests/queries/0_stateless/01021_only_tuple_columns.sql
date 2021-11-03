-- Tags: no-parallel

CREATE TABLE test
(
    `x` Tuple(UInt64, UInt64)
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO test SELECT (number, number) FROM numbers(1000000);

SELECT COUNT() FROM test;

ALTER TABLE test DETACH PARTITION tuple();

ALTER TABLE test ATTACH PARTITION tuple();

SELECT COUNT() FROM test;

DETACH TABLE test;

ATTACH TABLE test;

SELECT COUNT() FROM test;

DROP TABLE test;
