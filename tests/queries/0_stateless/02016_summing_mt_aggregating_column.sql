DROP TABLE IF EXISTS summing_mt_aggregating_column;

CREATE TABLE summing_mt_aggregating_column
(
    Key UInt64,
    Value UInt64,
    ConcatArray SimpleAggregateFunction(groupArrayArray, Array(UInt64))
)
ENGINE = SummingMergeTree()
ORDER BY Key;

INSERT INTO summing_mt_aggregating_column VALUES (1, 2, [333, 444]);
INSERT INTO summing_mt_aggregating_column VALUES (1, 3, [555, 999]);

OPTIMIZE TABLE summing_mt_aggregating_column FINAL;

SELECT * FROM summing_mt_aggregating_column;

DROP TABLE IF EXISTS summing_mt_aggregating_column;
