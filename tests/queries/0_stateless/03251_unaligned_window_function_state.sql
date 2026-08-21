-- https://github.com/ClickHouse/ClickHouse/issues/70569
-- The aggregate form is now experimental. Without the opt-in it must fail
-- before allocating aggregate state, rather than reaching the old unaligned
-- window-state path.
SELECT anyLast(id), anyLast(time), exponentialTimeDecayedAvg(10)(id, time) FROM values('id Int8, time DateTime', (1,1),(1,2),(2,3),(3,3),(3,5)); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
