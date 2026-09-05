-- https://github.com/ClickHouse/ClickHouse/issues/70569
SET allow_experimental_time_decay_aggregate_functions = 1;

-- Exercise the aggregate state itself. This used to expose an unaligned state
-- access under UBSan; merely checking the disabled-feature error would not.
SELECT
    anyLast(id),
    toUInt32(anyLast(time)),
    isFinite(exponentialTimeDecayedAvg(10)(id, time))
FROM values('id Int8, time DateTime', (1,1),(1,2),(2,3),(3,3),(3,5));
