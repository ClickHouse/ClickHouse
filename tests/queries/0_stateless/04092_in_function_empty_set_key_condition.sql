DROP TABLE IF EXISTS test;

CREATE TABLE test (d Date32)
ENGINE = MergeTree ORDER BY (toYear(d), toDate(d));

INSERT INTO test VALUES ('2020-01-01');

SELECT count() FROM test WHERE d IN (SELECT toDate32(number) FROM numbers(0));
