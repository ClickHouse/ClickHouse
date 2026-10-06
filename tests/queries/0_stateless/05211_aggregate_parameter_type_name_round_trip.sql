-- A Decimal or wide-integer parameter has no bare SQL literal form, so the printed state type name
-- needs a ::Type suffix. That name is what lands in the table metadata, and it is reparsed on every
-- start, so a name that does not parse back into the same parameters leaves the table unreadable.

DROP TABLE IF EXISTS t_ap_05211;

CREATE TABLE t_ap_05211
(
    control_ema_float64     AggregateFunction(exponentialMovingAverage(0.5), Float64, UInt64),
    control_topk_uint64     AggregateFunction(topK(3), UInt64),
    ema                     AggregateFunction(exponentialMovingAverage(0.5::Decimal32(1)), Float64, UInt64),
    ml_method               AggregateFunction(stochasticLinearRegression(0.01::Decimal32(2)), Float64, Float64),
    numeric_indexed_vector  AggregateFunction(groupNumericIndexedVector('BSI', 32::Decimal32(0), 0::Decimal32(0)), UInt32, Float64),
    topk_generic            AggregateFunction(topK(3::Decimal32(0)), String),
    topk_num                AggregateFunction(topK(3::Decimal32(0)), UInt64),
    topk_tuple              AggregateFunction(topKTuple(3::Decimal32(0)), Tuple(UInt64)),
    uniq_combined           AggregateFunction(uniqCombined(15::Decimal32(0)), UInt64),
    uniq_combined_variadic  AggregateFunction(uniqCombined(15::Decimal32(0)), UInt64, UInt64),
    uniq_up_to              AggregateFunction(uniqUpTo(5::Int256), UInt64),
    uniq_up_to_variadic     AggregateFunction(uniqUpTo(5::Int256), UInt64, UInt64)
)
ENGINE = MergeTree ORDER BY tuple();

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 't_ap_05211'
ORDER BY name;

INSERT INTO t_ap_05211 VALUES (
    initializeAggregation('exponentialMovingAverageState(0.5)', 1.0, 1::UInt64),
    initializeAggregation('topKState(3)', 2::UInt64),
    initializeAggregation('exponentialMovingAverageState(0.5::Decimal32(1))', 3.0, 1::UInt64),
    initializeAggregation('stochasticLinearRegressionState(0.01::Decimal32(2))', 4.0, 1.0),
    initializeAggregation('groupNumericIndexedVectorState(\'BSI\', 32::Decimal32(0), 0::Decimal32(0))', 5::UInt32, 1.0),
    initializeAggregation('topKState(3::Decimal32(0))', 'six'),
    initializeAggregation('topKState(3::Decimal32(0))', 7::UInt64),
    initializeAggregation('topKTupleState(3::Decimal32(0))', tuple(8::UInt64)),
    initializeAggregation('uniqCombinedState(15::Decimal32(0))', 9::UInt64),
    initializeAggregation('uniqCombinedState(15::Decimal32(0))', 10::UInt64, 10::UInt64),
    initializeAggregation('uniqUpToState(5::Int256)', 11::UInt64),
    initializeAggregation('uniqUpToState(5::Int256)', 12::UInt64, 12::UInt64));

-- Re-reads and re-parses the metadata, which is what the server does for every table on startup.
DETACH TABLE t_ap_05211;
ATTACH TABLE t_ap_05211;

SELECT count() FROM t_ap_05211;
SELECT finalizeAggregation(topk_num), finalizeAggregation(control_topk_uint64) FROM t_ap_05211;

DROP TABLE t_ap_05211;

-- groupArrayInsertAt keeps untyped parameters: its first one is a default value that can itself be
-- composite, and a suffix inside a composite does not parse back. Its name must not gain one.
DROP TABLE IF EXISTS t_gaia_05211;

CREATE TABLE t_gaia_05211 ENGINE = MergeTree ORDER BY tuple() AS
SELECT groupArrayInsertAtState([toDecimal32(1, 0)], 3)(x, p) AS s
FROM (SELECT [toDecimal32(1, 0)] AS x, toUInt8(0) AS p);

SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_gaia_05211';

DETACH TABLE t_gaia_05211;
ATTACH TABLE t_gaia_05211;

SELECT count() FROM t_gaia_05211;

DROP TABLE t_gaia_05211;

-- Folding a constant argument reaches the same printer with no type-spec syntax in the query.
SELECT toTypeName(exponentialMovingAverageState(toDecimal32(0.5, 1))(number, number)) FROM numbers(1);
