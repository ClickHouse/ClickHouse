-- The aggregate function factory removes LowCardinality from the argument types, so a state over a
-- type that mixes Array with a nested LowCardinality must serialize as its plain counterpart.

SELECT toTypeName(minState(materialize(['a']::Array(LowCardinality(String)))));
SELECT toTypeName(anyState(materialize((toLowCardinality('a'), [1::UInt8]))));

SELECT hex(minState(materialize(['a']::Array(LowCardinality(String)))));
SELECT hex(maxState(materialize([['a']::Array(LowCardinality(String))])));
SELECT hex(anyState(materialize((toLowCardinality('a'), [1::UInt8]))));
SELECT hex(anyLastState(materialize(map('k', ['a']::Array(LowCardinality(String))))));
SELECT hex(anyHeavyState(materialize(['a']::Array(LowCardinality(String)))));
SELECT hex(argMinState(materialize(['a']::Array(LowCardinality(String))), materialize(1)));
SELECT hex(maxArgMaxState(materialize(['a']::Array(LowCardinality(String))), materialize(1)));

SELECT hex(minState(x) OVER ()) FROM (SELECT ['a']::Array(LowCardinality(String)) AS x);
SELECT hex(arrayReduce('minState', [['a']::Array(LowCardinality(String))]));
SELECT hex(initializeAggregation('minState', ['a']::Array(LowCardinality(String))));

-- A column declared with the LowCardinality spelling keeps it in the type name, and its states can
-- still be written, merged and finalized.
CREATE TABLE states
(
    k UInt8,
    s AggregateFunction(min, Array(LowCardinality(String))),
    s2 AggregateFunction(any, Tuple(LowCardinality(String), Array(UInt8)))
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO states
SELECT
    number % 3,
    minState(materialize([toString(number)]::Array(LowCardinality(String)))),
    anyState(materialize((toLowCardinality(toString(number)), [toUInt8(number)])))
FROM numbers(30)
GROUP BY number % 3;

SELECT k, finalizeAggregation(s), finalizeAggregation(s2) FROM states ORDER BY k;
SELECT minMerge(s), anyMerge(s2) FROM states;
