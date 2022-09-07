DROP TABLE IF EXISTS stored_aggregates;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE stored_aggregates
(
    d Date,
    Uniq AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO stored_aggregates
SELECT
    toDate('2014-06-01') AS d,
    uniqState(number) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
);

SELECT uniqMerge(Uniq) FROM stored_aggregates;

DROP TABLE stored_aggregates;
