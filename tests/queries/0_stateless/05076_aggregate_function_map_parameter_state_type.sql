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
