-- An aggregate function is always instantiated with LowCardinality stripped from its arguments, but the
-- declared type keeps it. Both spellings describe the same state and must be accepted interchangeably.

CREATE TABLE states
(
    k UInt8,
    argmax AggregateFunction(argMax, LowCardinality(String), DateTime),
    argmax_nullable AggregateFunction(argMax, LowCardinality(Nullable(String)), DateTime),
    summap AggregateFunction(sumMap, Array(LowCardinality(String)), Array(UInt64)),
    quantiles AggregateFunction(quantiles(0.5), LowCardinality(Float64)),
    argmaxif AggregateFunction(argMaxIf, LowCardinality(Nullable(String)), DateTime, Bool)
)
ENGINE = MergeTree ORDER BY k;

-- Omitted columns are filled with the default state.
INSERT INTO states (k) VALUES (1);

INSERT INTO states SELECT
    2,
    argMaxState(toLowCardinality('a'), toDateTime(1)),
    argMaxState(toLowCardinality(CAST('b', 'Nullable(String)')), toDateTime(1)),
    sumMapState([toLowCardinality('x')], [toUInt64(7)]),
    quantilesState(0.5)(toLowCardinality(toFloat64(3))),
    argMaxIfState(toLowCardinality(CAST('c', 'Nullable(String)')), toDateTime(1), true);

OPTIMIZE TABLE states FINAL;

SELECT argMaxMerge(argmax), argMaxMerge(argmax_nullable), sumMapMerge(summap), quantilesMerge(0.5)(quantiles), argMaxIfMerge(argmaxif) FROM states;

-- The default state is the same one the spelling without LowCardinality produces.
SELECT hex(defaultValueOfTypeName('AggregateFunction(argMax, LowCardinality(String), DateTime)'))
     = hex(defaultValueOfTypeName('AggregateFunction(argMax, String, DateTime)'));

-- States are interchangeable between the two spellings.
CREATE TABLE without_low_cardinality (argmax AggregateFunction(argMax, String, DateTime)) ENGINE = Memory;
INSERT INTO without_low_cardinality SELECT argmax FROM states;
INSERT INTO states (k, argmax) SELECT 3, argmax FROM without_low_cardinality;
SELECT argMaxMerge(argmax) FROM states;
