DROP TABLE IF EXISTS stored_aggregates;

CREATE TABLE stored_aggregates
(
    d   Date,
    Uniq        AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO stored_aggregates
SELECT
    toDate(toUInt16(toDate('2014-06-01')) + intDiv(number, 100)) AS d,
    uniqState(intDiv(number, 10)) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
)
GROUP BY d;

SELECT uniqMerge(Uniq) FROM stored_aggregates;

SELECT d, uniqMerge(Uniq) FROM stored_aggregates GROUP BY d ORDER BY d;

INSERT INTO stored_aggregates
SELECT
    toDate(toUInt16(toDate('2014-06-01')) + intDiv(number, 100)) AS d,
    uniqState(intDiv(number + 50, 10)) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 500, 1000
)
GROUP BY d;

SELECT uniqMerge(Uniq) FROM stored_aggregates;

SELECT d, uniqMerge(Uniq) FROM stored_aggregates GROUP BY d ORDER BY d;

OPTIMIZE TABLE stored_aggregates;

SELECT uniqMerge(Uniq) FROM stored_aggregates;

SELECT d, uniqMerge(Uniq) FROM stored_aggregates GROUP BY d ORDER BY d;

DROP TABLE stored_aggregates;

