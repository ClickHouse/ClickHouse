-- The sibling of the state type of a bare `argMin`: a combinator and the adapter for `Nullable`
-- arguments are constructed with the caller's parameters, so `argMinIfState(map(1, 2))(...)` wrote
-- `AggregateFunction(argMinIf([(1, 2)]), ...)` into the table metadata - a type the parser rejects
-- on `ATTACH`, leaving the table loadable only after the metadata file is edited by hand.

SELECT toTypeName(argMinIfState(map(1, 2))(number, number, number > 0)) FROM numbers(1);
SELECT toTypeName(argMinState(map(1, 2))(toNullable(number), number)) FROM numbers(1);
SELECT toTypeName(argMinIfOrNullState(map(1, 2))(number, number, number > 0)) FROM numbers(1);
SELECT toTypeName(argMinDistinctState(map(1, 2))(number, number)) FROM numbers(1);
SELECT toTypeName(intervalLengthSumIfState(map(1, 2))(number::Float64, (number + 2)::Float64, number > 0)) FROM numbers(1);
SELECT toTypeName(argMinTupleState(map(1, 2))(tuple(number, number), tuple(number, number))) FROM numbers(1);
SELECT toTypeName(argMinTupleState(map(1, 2))(tuple(NULL, number), tuple(NULL, number))) FROM numbers(1);

SELECT 'a combinator that reads parameters keeps them';
SELECT toTypeName(quantilesIfState(0.5)(number, number > 0)) FROM numbers(1);
SELECT toTypeName(argMinResampleState(0, 2, 1)(number, number, number)) FROM numbers(1);
SELECT toTypeName(quantilesResampleState(0.5, 0, 2, 1)(number, number)) FROM numbers(1);
SELECT toTypeName(quantilesTupleState(0.5)(tuple(number, number))) FROM numbers(1);

SELECT 'a combinator with parameters of its own keeps only those';
SELECT toTypeName(argMinResampleState(map(1, 2), 0, 2, 1)(number, number, number)) FROM numbers(1);
SELECT toTypeName(argMinResampleIfState(map(1, 2), 0, 2, 1)(number, number, number, number > 0)) FROM numbers(1);

SELECT 'the table round-trips';
DROP TABLE IF EXISTS t_wrapped_map_param;
CREATE TABLE t_wrapped_map_param ENGINE = MergeTree ORDER BY tuple()
    AS SELECT argMinIfState(map(1, 2))(number, number, number > 0) AS s FROM numbers(3);
DETACH TABLE t_wrapped_map_param;
ATTACH TABLE t_wrapped_map_param;
SELECT argMinIfMerge(s) FROM t_wrapped_map_param;
DROP TABLE t_wrapped_map_param;

DROP TABLE IF EXISTS t_nullable_map_param;
CREATE TABLE t_nullable_map_param ENGINE = MergeTree ORDER BY tuple()
    AS SELECT argMinState(map(1, 2))(toNullable(number), number) AS s FROM numbers(3);
DETACH TABLE t_nullable_map_param;
ATTACH TABLE t_nullable_map_param;
SELECT argMinMerge(s) FROM t_nullable_map_param;
DROP TABLE t_nullable_map_param;

DROP TABLE IF EXISTS t_resample_map_param;
CREATE TABLE t_resample_map_param ENGINE = MergeTree ORDER BY tuple()
    AS SELECT argMinResampleState(map(1, 2), 0, 2, 1)(number, number, number) AS s FROM numbers(3);
DETACH TABLE t_resample_map_param;
ATTACH TABLE t_resample_map_param;
SELECT argMinResampleMerge(0, 2, 1)(s) FROM t_resample_map_param;
DROP TABLE t_resample_map_param;
