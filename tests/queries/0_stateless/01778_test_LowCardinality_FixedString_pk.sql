DROP TABLE IF EXISTS test_01778;

CREATE TABLE test_01778
(
    `key` LowCardinality(FixedString(3)),
    `d` date
)
ENGINE = MergeTree(d, key, 8192);


INSERT INTO test_01778 SELECT toString(intDiv(number,8000)), today() FROM numbers(100000);
INSERT INTO test_01778 SELECT toString('xxx'), today() FROM numbers(100);

SELECT count() FROM test_01778 WHERE key = 'xxx';

SELECT count() FROM test_01778 WHERE key = toFixedString('xxx', 3);

SELECT count() FROM test_01778 WHERE toString(key) = 'xxx';

DROP TABLE test_01778;

