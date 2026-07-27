DROP TABLE IF EXISTS agg_table;

CREATE TABLE IF NOT EXISTS agg_table
(
    time DateTime CODEC(DoubleDelta, LZ4),
    xxx String,
    two_values Tuple(Array(UInt16), UInt32),
    agg_simple SimpleAggregateFunction(sum, UInt64),
    agg SimpleAggregateFunction(sumMap, Tuple(Array(Int16), Array(UInt64)))
)
ENGINE = AggregatingMergeTree()
ORDER BY (xxx, time);

INSERT INTO agg_table SELECT toDateTime('2020-10-01 19:20:30'), 'hello', ([any(number)], sum(number)), sum(number),
    sumMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))) FROM numbers(10);

SELECT * FROM agg_table;

SELECT if(xxx = 'x', ([2], 3), ([3], 4)) FROM agg_table;

SELECT if(xxx = 'x', ([2], 3), ([3], 4, 'q', 'w', 7)) FROM agg_table; --{ serverError NO_COMMON_TYPE }

ALTER TABLE agg_table UPDATE two_values = (two_values.1, two_values.2) WHERE time BETWEEN toDateTime('2020-08-01 00:00:00') AND toDateTime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

ALTER TABLE agg_table UPDATE agg_simple = 5 WHERE time BETWEEN toDateTime('2020-08-01 00:00:00') AND toDateTime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

ALTER TABLE agg_table UPDATE agg = (agg.1, agg.2) WHERE time BETWEEN toDateTime('2020-08-01 00:00:00') AND toDateTime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

ALTER TABLE agg_table UPDATE agg = (agg.1, arrayMap(x -> toUInt64(x / 2), agg.2)) WHERE time BETWEEN toDateTime('2020-08-01 00:00:00') AND toDateTime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

SELECT * FROM agg_table;

DROP TABLE IF EXISTS agg_table;
