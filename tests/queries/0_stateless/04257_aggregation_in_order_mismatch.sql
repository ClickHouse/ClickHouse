DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    a String,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test
SELECT toString(rand() % 100000), number FROM numbers(300000);
INSERT INTO test
SELECT toString(rand() % 100000), number FROM numbers(300000);

SET optimize_aggregation_in_order = 1;

SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test GROUP BY a
         SETTINGS optimize_aggregation_in_order = 0))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test GROUP BY a
         SETTINGS optimize_aggregation_in_order = 0));

DROP TABLE test;
