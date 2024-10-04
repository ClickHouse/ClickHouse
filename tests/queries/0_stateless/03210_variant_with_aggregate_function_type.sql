SET allow_experimental_variant_type = 1;

DROP TABLE IF EXISTS source;
CREATE TABLE source 
(
   Name String,
   Value Int64

) ENGINE = MergeTree ORDER BY ();

INSERT INTO source SELECT ['fail', 'success'][number % 2] as Name, number AS Value FROM numbers(1000);

DROP TABLE IF EXISTS test_agg_variant;
CREATE TABLE test_agg_variant
(
   Name String,
   Value Variant(AggregateFunction(uniqExact, Int64), AggregateFunction(avg, Int64))
)
ENGINE = MergeTree
ORDER BY (Name);

INSERT INTO test_agg_variant
SELECT
    Name,
    t AS Value
FROM
(
    SELECT
        Name,
        arrayJoin([
            uniqExactState(Value)::Variant(AggregateFunction(uniqExact, Int64), AggregateFunction(avg, Int64)), 
            avgState(Value)::Variant(AggregateFunction(uniqExact, Int64), AggregateFunction(avg, Int64))
        ]) AS t
    FROM source
    GROUP BY Name 
);

SELECT
    Name,
    uniqExactMerge(Value.`AggregateFunction(uniqExact, Int64)`) AS Value
FROM test_agg_variant
GROUP BY Name;

SELECT
    Name,
    avgMerge(Value.`AggregateFunction(avg, Int64)`) AS Value
FROM test_agg_variant
GROUP BY Name;

SELECT
    Name,
    uniqExactMerge(Value.`AggregateFunction(uniqExact, Int64)`) AS ValueUniq,
    avgMerge(Value.`AggregateFunction(avg, Int64)`) AS ValueAvg
FROM test_agg_variant
GROUP BY Name;


DROP TABLE test_agg_variant;
DROP TABLE source;

