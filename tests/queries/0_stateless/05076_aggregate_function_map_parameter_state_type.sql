-- A `Map` parameter renders as `[(1, 2)]`, which the `AggregateFunction` type parser does not accept
-- as a parameter. `argMin`, `argMax` and `intervalLengthSum` never read their parameters, so the
-- parameters stay out of the state type and the table's own metadata stays loadable.
SELECT toTypeName(argMinState(map(1, 2))(number, number)) FROM numbers(1);
SELECT toTypeName(intervalLengthSumState(map(1, 2))(number::Float64, (number + 2)::Float64)) FROM numbers(1);

DROP TABLE IF EXISTS argmin_map_param;
CREATE TABLE argmin_map_param ENGINE = MergeTree ORDER BY tuple()
    AS SELECT argMinState(map(1, 2))(number, number) AS s FROM numbers(3);
DETACH TABLE argmin_map_param;
ATTACH TABLE argmin_map_param;
SELECT argMinMerge(s) FROM argmin_map_param;
DROP TABLE argmin_map_param;

-- The state type name also travels with every state serialized into a `Field`, so such a state must
-- still be insertable into a column of its own type,
DROP TABLE IF EXISTS argmin_map_param_field;
CREATE TABLE argmin_map_param_field (s AggregateFunction(argMin, UInt8, UInt8)) ENGINE = Memory;
INSERT INTO argmin_map_param_field VALUES (arrayReduce('argMinState(map(1, 2))', [1, 2], [1, 2]));
SELECT argMinMerge(s) FROM argmin_map_param_field;
DROP TABLE argmin_map_param_field;

-- and `Dynamic` must be able to derive the variant type from it.
SELECT dynamicType(x) FROM VALUES('x Dynamic', (arrayReduce('argMinState(map(1, 2))', [1, 2], [1, 2])));
SELECT dynamicType(x) FROM VALUES('x Dynamic', (arrayReduce('intervalLengthSumState(map(1, 2))', [1., 2.], [3., 4.])));

-- Parameters that the function does read travel in the spelling the state type uses. Dropping the
-- `::Int64` or `::Decimal64` suffix reparses the name into a type the state does not belong to.
SELECT dynamicType(x) FROM VALUES('x Dynamic', (initializeAggregation('groupArrayMovingSumState(42::Int64)', 1::Int64)));
SET enable_time_series_aggregate_functions = 1;
SELECT dynamicType(x) FROM VALUES('x Dynamic', (initializeAggregation('timeSeriesInstantRateToGridState(toDateTime64(1734004810, 3), toDateTime64(1734004860, 3), 10, 60)', [toDateTime64(1734004810, 3)], [1.0])));

-- Upgrade compatibility: a column whose metadata still carries the parameters keeps accepting states
-- through a `Field` as well.
DROP TABLE IF EXISTS ils_legacy_param;
CREATE TABLE ils_legacy_param (s AggregateFunction(intervalLengthSum('two-sided', 1), Float64, Float64)) ENGINE = Memory;
INSERT INTO ils_legacy_param VALUES (arrayReduce('intervalLengthSumState', [1., 2.], [3., 4.]));
SELECT intervalLengthSumMerge(s) FROM ils_legacy_param;
DROP TABLE ils_legacy_param;
