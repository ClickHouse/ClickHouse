DROP TABLE IF EXISTS test_grace_hash;

CREATE TABLE test_grace_hash (id UInt32, value UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_grace_hash SELECT number, number % 100 = 0 FROM numbers(100000);

SET join_algorithm = 'grace_hash';

SELECT count() FROM (
    SELECT f.id FROM test_grace_hash AS f
    LEFT JOIN test_grace_hash AS d
    ON f.id = d.id
    LIMIT 1000
);

DROP TABLE test_grace_hash;
