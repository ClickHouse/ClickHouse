DROP TABLE IF EXISTS summing_mt_aggregating_column;

CREATE TABLE summing_mt_aggregating_column
(
    Key UInt64,
    Value UInt64,
    ConcatArraySimple SimpleAggregateFunction(groupArrayArray, Array(UInt64)),
    ConcatArrayComplex AggregateFunction(groupArrayArray, Array(UInt64))
)
ENGINE = SummingMergeTree()
ORDER BY Key;

INSERT INTO summing_mt_aggregating_column SELECT 1, 2, [333, 444], groupArrayArrayState([toUInt64(33), toUInt64(44)]);
INSERT INTO summing_mt_aggregating_column SELECT 1, 3, [555, 999], groupArrayArrayState([toUInt64(55), toUInt64(99)]);

OPTIMIZE TABLE summing_mt_aggregating_column FINAL;

SELECT Key, any(Value), any(ConcatArraySimple), groupArrayArrayMerge(ConcatArrayComplex) FROM summing_mt_aggregating_column GROUP BY Key;

DROP TABLE IF EXISTS summing_mt_aggregating_column;
