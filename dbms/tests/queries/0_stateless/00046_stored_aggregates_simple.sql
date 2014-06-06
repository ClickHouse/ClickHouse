DROP TABLE IF EXISTS test.stored_aggregates;

CREATE TABLE test.stored_aggregates
(
    d Date,
    Uniq AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO test.stored_aggregates
SELECT
    toDate('2014-06-01') AS d,
    uniqState(number) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
);

SELECT uniqMerge(Uniq) FROM test.stored_aggregates;

DROP TABLE test.stored_aggregates;
